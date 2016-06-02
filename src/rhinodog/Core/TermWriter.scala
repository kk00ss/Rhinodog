package rhinodog.Core

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import Definitions._
import Configuration._
import Iterators._
import rhinodog.Core.Utils._

import scala.collection._

class TermWriter
(config: TermWriterConfig) {
    import config._
    val serializer = MetadataSerializer(mainComponents.measureSerializer)

    val blocksManager = new BlocksWriter(mainComponents.measureSerializer,
                                             config.termID,
                                             config.targetSize)

    //for small flush interval during which analysis threads may add documents out of order
    private var writeCache = SortedSet[DocPosting]()
    private var minDocID = Long.MaxValue
    private var maxDocID = 0l

    //probably a bit misplaced lock, it controls access to term metadata in MetadataManager
    //for new trees and for compactions, that is why we need withLock functions
    private val addOrFlushLock = new ReentrantLock()

    def getIterator = { new RowSegmentIterator(writeCache.toSeq.sortBy(_.docID)) }

    def withLock(func: Function0[Unit]) = {
        addOrFlushLock.lock()
        try {
            func()
        } finally { addOrFlushLock.unlock() }
    }

    def tryWithLock(func: Function0[Unit], timeout: Long = 0): Boolean = {
        val attemptResult = addOrFlushLock.tryLock(timeout, TimeUnit.MILLISECONDS)
        if(!attemptResult) return false
        else {
            try {
                func()
            } finally { addOrFlushLock.unlock() }
        }
        return true
    }

    //TODO: investigate different timeouts settings
    /** use increasing timeouts for last terms of the document to finnish it's */
    def tryAdd(doc: DocPosting, timeout: Long = 0): Boolean = {
        val attemptResult = addOrFlushLock.tryLock(timeout, TimeUnit.MILLISECONDS)
        if(!attemptResult)
            return false
        try {
            //Analysis threads could finnish processing in different order than they started
            if(doc.docID < minDocID) minDocID = doc.docID
            if(doc.docID > maxDocID) maxDocID = doc.docID
            writeCache += doc
            return true
        } finally { addOrFlushLock.unlock() }
    }

    def smallFlush() = {
        addOrFlushLock.lock()
        try {
            if (writeCache.nonEmpty) {
                writeCache.foreach(blocksManager.add)
                writeCache = SortedSet[DocPosting]()
            }
        } finally { addOrFlushLock.unlock()}
    }

    def largeFlush() = {
        addOrFlushLock.lock()
        try {
            val blocks = blocksManager.flushBlocks()
            for(block <- blocks) {
                val key = block.key
                val metaSerialized = serializer.serialize(block.meta)
                mainComponents.repository.saveBlock(key, block.data, metaSerialized)
                mainComponents.metadataManager.addMetadata(key, block.meta)
            }
        } finally { addOrFlushLock.unlock() }
    }
}