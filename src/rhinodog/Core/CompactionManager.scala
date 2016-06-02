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
class CompactionManager
(config: GlobalConfig,
 dependencies: MainComponents) extends CompactionManagerInterface {

    import config._

    //TODO: track LMDB map free space ratio rather than FS's
    //private var freeSpaceRatio: Float = 0
    //private var minCompactionSize: Int = 2
    def updateFreeSpaceRatio(newValue: Float) = {
        //ignoring updates and only merging two blocks at a time
        //this.freeSpaceRatio = newValue
        //we can choose to merge more blocks with smaller fraction of deleted documents

        //val effectiveFSR = if(freeSpaceRatio >= 0.5) freeSpaceRatio
        //else 0.5f
        //minCompactionSize = math.round(1/effectiveFSR)
    }

    //special key = Int.MaxValue for trees that are larger than maxCompactionSize / compactionFactor
    val compactionInfo = new ConcurrentHashMap[Int, TermCompactionInfo]()

    case class TermCompactionInfo
    (var levelZero: TreeSet[BlockKey] = TreeSet(),
     //storing fill ratios for blocks
     var levelOne: TreeMap[BlockKey, Float] = TreeMap(),
     var zeroLevelSize: Int = 0)

    //(job, executeImmediately) => Unit
    var addCompactionJob: Function1[CompactionJobInterface, Unit] = null
    val separateThreadExecution = true

    //should be executed with TermWriterLock (from MetadataManager.addMetadata )
    def blockAddedEvent(termID: Int, key: BlockKey, meta: BlockMetadata) = {
        compactionInfo.putIfAbsent(termID, TermCompactionInfo())
        val termCompactionInfo = compactionInfo.get(termID)
        termCompactionInfo.zeroLevelSize += meta.encodedSize
        termCompactionInfo.levelZero += key
        if (termCompactionInfo.zeroLevelSize >= merges_MinSize) {
            val mergeJob = BaseCompactionJob(termID, termCompactionInfo.levelZero.toSeq)
            termCompactionInfo.levelZero = TreeSet()
            termCompactionInfo.zeroLevelSize = 0
            addCompactionJob(mergeJob)
        }
    }

    def docDeletedEvent(key: BlockKey, meta: BlockMetadata) = {
        compactionInfo.putIfAbsent(key.termID, TermCompactionInfo())
        val termCompactionInfo = compactionInfo.get(key.termID)
        termCompactionInfo.levelOne += ((key, meta.fillFactor))
        val fillFactorThreshold = 1f / merges_CompactionFactor
        if (meta.fillFactor < fillFactorThreshold) {
            val keys = termCompactionInfo.levelOne
                .iteratorFrom(key)
                .takeWhile(_._2 < fillFactorThreshold)
                .map(_._1)
            val estimatedCompactionSize = keys.length * pageSize
            if (estimatedCompactionSize >= merges_MinSize) {
                val nBlocks = merges_MaxSize / pageSize
                val keysToCompact = keys.take(nBlocks).toSeq
                val compactionJob = BaseCompactionJob(key.termID, keysToCompact)
                keysToCompact.foreach(key => termCompactionInfo.levelOne -= key)
                addCompactionJob(compactionJob)
            }
        }
    }


    case class BaseCompactionJob(termID: Int,
                                 keys: Seq[BlockKey]) extends CompactionJobInterface {
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
                config.targetBlockSize)
            for (posting <- data)
                blocksManager.add(posting)
            blocksManager.flushBlocks()
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
            //TODO: maybe add block with underflow to levelZero
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
                        //TODO: maybe add block with underflow to levelZero
                        blocks.foreach(block => {
                            termCompactionInfo.levelOne += ((block.key, block.meta.fillFactor))
                        })
                    })
                })
            }
    }

}