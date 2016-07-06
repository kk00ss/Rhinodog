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
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import javax.activation.DataSource

import org.slf4j.LoggerFactory
import rhinodog.Core.Definitions.Caching.BlockCache

import scala.collection._

import rhinodog.Core.Definitions._
import Configuration._
import BaseTraits._
import rhinodog.Core.Utils._

import scala.collection.immutable.{TreeSet, HashSet, TreeMap}
import scala.collection.mutable.ArrayBuffer

import scala.collection.JavaConversions.{asScalaIterator, mapAsScalaMap}

//TWO PHAZE COPACTION
// new blocks are of different sizes and are of compaction level 0
// 1-st PHAZE - consolidate data in blocks of pageSize - merges
// 2-nd PHAZE - reclaim space used by deleted docs - compactions

//TODO: test behaviour when storage is already closed
class CompactionManager(dependencies: MainComponents) extends ICompactionManager {
    private val logger = LoggerFactory.getLogger(this.getClass)
    import GlobalConfig._


    //special key = Int.MaxValue for trees that are larger than maxCompactionSize / compactionFactor
    val compactionInfo = new ConcurrentHashMap[Int, TermCompactionInfo]()

    private val _totalLevelOneSize = dependencies.metrics.counter("TotalSize LevelOne")
    //size of data on zero level, without space amplification
    private val _totalZeroLevelSize = dependencies.metrics.counter("TotalSize ZeroLevel")
    private val _computeChangesTimer = dependencies.metrics.timer("ComputeChanges")
    private val _saveChangesTimer = dependencies.metrics.timer("SaveChanges")
    private val _blockAddTimer = dependencies.metrics.timer("BlockAdd")
    private val _docDeletedTimer = dependencies.metrics.timer("DocDeleted")

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
        logger.debug("-> blockAddedEvent key = {}",key)
        logger.trace("blockAddedEvent meta = {}",meta)
        val context = _blockAddTimer.time()
        compactionInfo.putIfAbsent(termID, TermCompactionInfo())
        val termCompactionInfo = compactionInfo.get(termID)
        termCompactionInfo.zeroLevelSize += meta.encodedSize
        _totalZeroLevelSize.inc(meta.encodedSize)
        termCompactionInfo.levelZero += key
        if (termCompactionInfo.zeroLevelSize >= merges_minSize.get()) {
            logger.info("blockAddedEvent creating new zero2one compaction for termID = {}, " +
                "zeroLevelSize = {}", termID, termCompactionInfo.zeroLevelSize)
            val mergeJob = BaseCompactionJob(termID,
                                             termCompactionInfo.levelZero.toSeq,
                                             termCompactionInfo.zeroLevelSize,
                                             true)
            termCompactionInfo.levelZero = TreeSet()
            termCompactionInfo.zeroLevelSize = 0
            addCompactionJob(mergeJob)
        }
        context.stop()
    }

    //should be executed with TermWriterLock (from MetadataManager.deleteFromTerm )
    def docDeletedEvent(key: BlockKey, meta: BlockMetadata) = {
        logger.debug("-> docDeletedEvent key = {}",key)
        val context = _docDeletedTimer.time()
        compactionInfo.putIfAbsent(key.termID, TermCompactionInfo())
        val termCompactionInfo = compactionInfo.get(key.termID)
        termCompactionInfo.levelOne += ((key, meta.fillFactor))
        val fillFactorThreshold = 1f / merges_compactionFactor.get()
        if (meta.fillFactor < fillFactorThreshold) {
            logger.trace("docDeletedEvent fillFactor is smaller than threshold = {}", meta.fillFactor)
            val keys = termCompactionInfo.levelOne
                .iteratorFrom(key)
                .takeWhile(_._2 < fillFactorThreshold)
                .map(_._1)
            val estimatedCompactionSize = keys.length * storage_pageSize
            if (estimatedCompactionSize >= merges_minSize.get()) {
                logger.trace("docDeletedEvent estimatedCompactionSize >= merges_minSzie = {}",
                                estimatedCompactionSize)
                val nBlocks = merges_maxSize.get() / storage_pageSize
                val keysToCompact = keys.take(nBlocks).toSeq
                val compactionJob = BaseCompactionJob(key.termID,
                                                      keysToCompact,
                                                      estimatedCompactionSize,
                                                      false)
                keysToCompact.foreach(key => termCompactionInfo.levelOne -= key)
                // Dummy mark that indicates that there are blocks in scheduled compaction
                // so this block (doc range) shouldn't be included in any compactions
                // until previous one is completed
                // (10 is larger than possible values of fillFactorThreshold)
                // should be removed before saving new blocks compaction info
                termCompactionInfo.levelOne += ((keysToCompact.last, 10))
                addCompactionJob(compactionJob)
            }
            context.stop()
        }
    }


    case class BaseCompactionJob(termID: Int,
                                 keys: Seq[BlockKey],
                                 estimatedCompactionSize: Long,
                                 //0 or 1
                                 sourceLevelIsZero: Boolean) extends ICompactionJob {
        val docPostingsSerializer = new DocPostingsSerializer(dependencies.measureSerializer)
        val serializer = MetadataSerializer(dependencies.measureSerializer)

        logger.trace("BaseCompactionJob created for keys = {}", keys)

        def computeChanges(): Function0[Unit] = {
            val context = _computeChangesTimer.time()
            logger.debug("-> BaseCompactionJob.computeChanges created for keys = {}", keys)
            val blocks = baseComputeChanges(keys)
            val ret = getSyncSaveChangesHook(keys, blocks)
            context.stop()
            logger.debug("BaseCompactionJob.computeChanges ->")
            ret
        }

        //PRIVATE SECTION
        //for testability
        def baseComputeChanges(keys: Seq[BlockKey]): Seq[Definitions.BlockInfoRAM] = {
            logger.trace("-> BaseCompactionJob.baseComputeChanges")
            val filteredData = getFilteredBlockData(keys)
            val blocksToSave = partitionIntoBlocks(filteredData)
            keysConflictsDetection(keys, blocksToSave)
            logger.trace("BaseCompactionJob.baseComputeChanges ->")
            blocksToSave
        }

        def getFilteredBlockData(keys: Iterable[Definitions.BlockKey]): Seq[DocPosting] = {
            logger.trace("-> BaseCompactionJob.computeChanges created for keys = {}", keys)
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
            logger.trace("-> BaseCompactionJob.partitionIntoBlocks")
            val blocksManager = new BlocksWriter(dependencies.measureSerializer,
                                                 termID,
                                                 storage_targetBlockSize)
            for (posting <- data)
                blocksManager.add(posting)
            blocksManager.flushBlocks().values.toSeq
        }


        def getSyncSaveChangesHook(keys: Seq[BlockKey],
                                   blocks: Seq[Definitions.BlockInfoRAM]): Function0[Unit]
        = () => saveChangesSync(keys, blocks)

        //NOT THREAD-SAFE SHOULD BE EXECUTED WITH STORAGE AND TERM-WRITER LOCKS
        //assumption is that keys is sorted as it was at the moment of scheduling
        def saveChangesSync(keys: Seq[BlockKey], blocks: Seq[Definitions.BlockInfoRAM]) = {
            logger.debug("-> BaseCompactionJob.saveChangesSync for keys = {}", keys)
            val context = _saveChangesTimer.time()
            val blocksCaches = blocks.map(block => {
                val metaSerialized = serializer.serialize(block.meta)
                BlockCache(block.key, block.data, metaSerialized)
            })
            dependencies.repository.replaceBlocks(keys, blocksCaches)
            val blocksMetas = blocks.map(x => (x.key, x.meta))
            dependencies.metadataManager.replaceMetadata(keys.head.termID, keys, blocksMetas)
            if(sourceLevelIsZero) _totalZeroLevelSize.dec(estimatedCompactionSize)
            else _totalLevelOneSize.dec(estimatedCompactionSize)
            _totalLevelOneSize.inc(blocks.size * storage_pageSize)
            //Saving compaction info about newly created blocks
            compactionInfo.putIfAbsent(termID, TermCompactionInfo())
            val termCompactionInfo = compactionInfo.get(termID)
            //removing dummy mark
            termCompactionInfo.levelOne -= keys.last
            blocks.foreach(block => {
                termCompactionInfo.levelOne += ((block.key, block.meta.fillFactor))
            })
            context.stop()
            logger.debug("BaseCompactionJob.saveChangesSync ->")
        }

        def keysConflictsDetection(keys: scala.Seq[BlockKey], blocksToSave: scala.Seq[BlockInfoRAM]): Unit = {
            if(logger.isTraceEnabled)
                logger.trace("-> BaseCompactionJob.keysConflictsDetection " +
                    "original keys = {}, new keys = {}", Array(keys, blocksToSave.map(_.key)))
            else logger.debug("-> BaseCompactionJob.keysConflictsDetection")
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
            logger.debug("BaseCompactionJob.keysConflictsDetection ->")
        }
    }

}
