// Copyright 2016 Konstantin Voznesenskiy

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rhinodog.Core

import java.nio.ByteBuffer
import java.util.concurrent._
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}

import rhinodog.Core.Definitions.Caching.BlockCache

import scala.collection._

import rhinodog.Core.Definitions._
import Configuration._
import BaseTraits._
import rhinodog.Core.Utils._

import scala.collection.immutable.{TreeSet, HashSet, TreeMap}
import scala.collection.mutable.ArrayBuffer

object PartialFlushWAL {
    def deserialize(buf: ByteBuffer, length: Int): PartialFlushWAL = {
        if (length % BlockKey.sizeInBytes != 0)
            throw new IllegalStateException("buffer of wrong size")
        val N = length / BlockKey.sizeInBytes
        val keys = (1 to N).map(_ => BlockKey.deserialize(buf))
        PartialFlushWAL(keys)
    }
}

case class PartialFlushWAL(blockKeys: Seq[BlockKey]) {
    def bytesRequired: Int = blockKeys.length * BlockKey.sizeInBytes

    def serialize(buf: ByteBuffer) = blockKeys.foreach(_.serialize(buf))
}

//TWO PHAZE COPACTION
// new blocks are of different sizes and are of compaction level 0
// 1-st PHAZE - consolidate data in blocks of pageSize - merges
// 2-nd PHAZE - reclaim space used by deleted docs - compactions

//TODO: test behaviour when storage is already closed
class CompactionManager(dependencies: MainComponents) extends ICompactionManager {
    import GlobalConfig._

    //special key = Int.MaxValue for trees that are larger than maxCompactionSize / compactionFactor
    val compactionInfo = new ConcurrentHashMap[Int, TermCompactionInfo]()

    case class TermCompactionInfo
    (var levelZero: TreeSet[BlockKey] = TreeSet(),
     //storing fill ratios for blocks
     var levelOne: TreeMap[BlockKey, Float] = TreeMap(),
     var zeroLevelSize: Int = 0)

    //(job, executeImmediately) => Unit
    var addCompactionJob: Function1[ICompactionJob, Unit] = null
    val separateThreadExecution = true

    //should be executed with TermWriterLock (from MetadataManager.addMetadata )
    def blockAddedEvent(termID: Int, key: BlockKey, meta: BlockMetadata) = {
        compactionInfo.putIfAbsent(termID, TermCompactionInfo())
        val termCompactionInfo = compactionInfo.get(termID)
        termCompactionInfo.zeroLevelSize += meta.encodedSize
        termCompactionInfo.levelZero += key
        if (termCompactionInfo.zeroLevelSize >= merges_minSize) {
            val mergeJob = BaseCompactionJob(termID, termCompactionInfo.levelZero.toSeq)
            termCompactionInfo.levelZero = TreeSet()
            termCompactionInfo.zeroLevelSize = 0
            addCompactionJob(mergeJob)
        }
    }

    //should be executed with TermWriterLock (from MetadataManager.deleteFromTerm )
    def docDeletedEvent(key: BlockKey, meta: BlockMetadata) = {
        compactionInfo.putIfAbsent(key.termID, TermCompactionInfo())
        val termCompactionInfo = compactionInfo.get(key.termID)
        termCompactionInfo.levelOne += ((key, meta.fillFactor))
        val fillFactorThreshold = 1f / merges_compactionFactor
        if (meta.fillFactor < fillFactorThreshold) {
            val keys = termCompactionInfo.levelOne
                .iteratorFrom(key)
                .takeWhile(_._2 < fillFactorThreshold)
                .map(_._1)
            val estimatedCompactionSize = keys.length * pageSize
            if (estimatedCompactionSize >= merges_minSize) {
                val nBlocks = merges_MaxSize / pageSize
                val keysToCompact = keys.take(nBlocks).toSeq
                val compactionJob = BaseCompactionJob(key.termID, keysToCompact)
                keysToCompact.foreach(key => termCompactionInfo.levelOne -= key)
                // Dummy mark that indicates that there are blocks in scheduled compaction
                // so this block (doc range) shouldn't be included in any compactions
                // until previous one is completed
                // (10 is larger than possible values of fillFactorThreshold)
                // should be removed before saving new blocks compaction info
                termCompactionInfo.levelOne += ((keysToCompact.last, 10))
                addCompactionJob(compactionJob)
            }
        }
    }


    case class BaseCompactionJob(termID: Int,
                                 keys: Seq[BlockKey]) extends ICompactionJob {
        val docPostingsSerializer = new DocPostingsSerializer(dependencies.measureSerializer)
        val serializer = MetadataSerializer(dependencies.measureSerializer)

        def computeChanges(): SaveChangesHook = {
            val blocks = baseComputeChanges(keys)
            getSyncSaveChangesHook(keys, blocks)
        }

        //PRIVATE SECTION

        def getFilteredBlockData(keys: Iterable[Definitions.BlockKey]): Seq[DocPosting] = {
            val snapshotReader = dependencies.repository.getSnapshotReader
            val blocks = keys.map(snapshotReader.getBlock)
            snapshotReader.close()
            val data = blocks.flatMap(block => {
                val blockBuffer = ByteBuffer.wrap(block)
                val postings = new mutable.ArrayBuffer[Definitions.DocPosting]()
                //irrelevant for compactions, but still want to preserve same format
                val _segmentMaxDocID = blockBuffer.getLong
                val _segmentMaxScore
                = dependencies.measureSerializer.deserialize(blockBuffer).score
                val segmentSkip = blockBuffer.getShort
                //end
                val segmentSerialized = DocPostingsSerializer.readComponents(blockBuffer)
                postings ++= docPostingsSerializer.decodeFromComponents(segmentSerialized)
                postings
            })
                .filterNot(doc => dependencies.metadataManager.isDeleted(doc.docID))
            data.toSeq
        }

        def partitionIntoBlocks(data: Seq[DocPosting]): Seq[BlockInfoRAM] = {
            val blocksManager = new BlocksWriter(dependencies.measureSerializer,
                                                 termID,
                                                 targetBlockSize)
            for (posting <- data)
                blocksManager.add(posting)
            blocksManager.flushBlocks().values.toSeq
        }


        def baseComputeChanges(keys: Seq[BlockKey]): Seq[Definitions.BlockInfoRAM] = {
            val filteredData = getFilteredBlockData(keys)
            val blocksToSave = partitionIntoBlocks(filteredData)
            keysConflictsDetection(keys, blocksToSave)
            blocksToSave
        }

        def getSyncSaveChangesHook(keys: Seq[BlockKey],
                                   blocks: Seq[Definitions.BlockInfoRAM]): SaveChangesHook =
            (withStorageLock: WithLockFunc, withTermWriterLock: WithLockFunc) =>
                withStorageLock(() => withTermWriterLock(() => saveChangesSync(keys, blocks)))

        //NOT THREAD-SAFE SHOULD BE EXECUTED WITH STORAGE AND TERM-WRITER LOCKS
        //assumption is that keys is sorted as it was at the moment of scheduling
        def saveChangesSync(keys: Seq[BlockKey], blocks: Seq[Definitions.BlockInfoRAM]) = {
            val blocksCaches = blocks.map(block => {
                val metaSerialized = serializer.serialize(block.meta)
                BlockCache(block.key, block.data, metaSerialized)
            })
            dependencies.repository.replaceBlocks(keys, blocksCaches)
            val blocksMetas = blocks.map(x => (x.key, x.meta))
            dependencies.metadataManager.replaceMetadata(keys.head.termID, keys, blocksMetas)
            //Saving compaction info about newly created blocks
            compactionInfo.putIfAbsent(termID, TermCompactionInfo())
            val termCompactionInfo = compactionInfo.get(termID)
            //removing dummy mark
            termCompactionInfo.levelOne -= keys.last
            blocks.foreach(block => {
                termCompactionInfo.levelOne += ((block.key, block.meta.fillFactor))
            })
        }

        def keysConflictsDetection(keys: scala.Seq[BlockKey], blocksToSave: scala.Seq[BlockInfoRAM]): Unit = {
            //blockKey conflict detection and resolution
            var lastOldKeysIndex = 0
            var newKeysIndex = 0
            while (newKeysIndex < blocksToSave.length) {
                val blockInfoRAM = blocksToSave(newKeysIndex)
                while (lastOldKeysIndex < keys.length && keys(lastOldKeysIndex) < blockInfoRAM.key)
                    lastOldKeysIndex += 1
                if (keys(lastOldKeysIndex) == blockInfoRAM.key)
                    blockInfoRAM.key.changeByte = (blockInfoRAM.key.changeByte + 1).asInstanceOf[Byte]

                newKeysIndex += 1
                while (newKeysIndex < blocksToSave.length &&
                    keys(lastOldKeysIndex) > blocksToSave(newKeysIndex).key)
                    newKeysIndex += 1
            }
        }

        //can save changes in multiply different LMDB transactions, using partialFlushWAL
        def saveChangesAsync(keys: Seq[BlockKey], _blocks: Seq[Definitions.BlockInfoRAM]): SaveChangesHook =
            (withStorageLock: WithLockFunc, withTermWriterLock: WithLockFunc) => {
                var blocks = _blocks
                val metadatas = new ArrayBuffer[(BlockKey, BlockMetadata)]()
                //async phase
                //number of blocks to write in same run
                val stepSize = merges_MaxSize / pageSize
                var keysForWAH = List[Array[Byte]]()
                while (blocks.nonEmpty) {
                    val blocksForStep = blocks.take(stepSize)
                    val keysForStep = blocksForStep.map(_.key)
                    val partialFlushWAH = PartialFlushWAL(keysForStep)
                    withStorageLock(() => {
                        withTermWriterLock(() => {
                            val keyForWAH = dependencies.repository
                                            .writePartialFlushWAL(partialFlushWAH)
                            keysForWAH ::= keyForWAH
                            for (block <- blocksForStep) {
                                val metaSerialized = serializer.serialize(block.meta)
                                // unreferenced data will not affect query execution,
                                // metadata would be reloaded on startup only if whole compaction
                                // will be correctly stored, otherwise metadata and data will be deleted
                                // due to PartialFlushWAL
                                dependencies.repository
                                    .saveBlock(block.key, block.data, metaSerialized)
                                metadatas += ((block.key, block.meta))
                            }
                        })
                    })
                    blocks = blocks.drop(stepSize)
                }
                //sync phase
                withStorageLock(() => {
                    withTermWriterLock(() => {
                        dependencies.repository
                            .replaceBlocks(keys, List())
                        dependencies.metadataManager
                            .replaceMetadata(keys.head.termID, keys, metadatas)
                        for (keyWAH <- keysForWAH)
                            dependencies.repository.deletePartialFlushWAL(keyWAH)
                        //Saving compaction info about newly created blocks
                        compactionInfo.putIfAbsent(termID, TermCompactionInfo())
                        val termCompactionInfo = compactionInfo.get(termID)
                        //removing dummy mark
                        termCompactionInfo.levelOne -= keys.last
                        blocks.foreach(block => {
                            termCompactionInfo.levelOne += ((block.key, block.meta.fillFactor))
                        })
                    })
                })
            }
    }

}
