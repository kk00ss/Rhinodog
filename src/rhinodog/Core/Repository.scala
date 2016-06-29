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

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock
import java.util.function.LongBinaryOperator

import org.fusesource.lmdbjni._
import org.slf4j.LoggerFactory

import rhinodog.Core.Definitions._
import BaseTraits._
import Caching._
import Configuration._

import scala.collection.JavaConversions.{asScalaIterator, mapAsScalaMap}
import scala.collection.Seq

class Repository(storageMode: storageModeEnum.storageMode,
                 //full names of measure, analyzer
                 watermark: String)
    extends IRepository with AutoCloseable {
    private val logger = LoggerFactory.getLogger(this.getClass)

    private var environment: Env = null

    private var postingsDB: Database = null
    private var metadataDB: Database = null
    private var documentsDB: Database = null
    private var numberOfDeletedDB: Database = null
    private var roaringBitmapsDB: Database = null
    private var partialFlushWAL_DB: Database = null

    private var term2ID_DB: Database = null
    private var ID2Term_DB: Database = null

    private var indexStatistics_DB: Database = null

    val numberOfDatabases = 16

    //TODO investigate re-usability of WriteCache instances
    var writeCache = new WriteCache()
    var dataBeingFlushed: WriteCache = null

    val writeCacheSHLock = new ReentrantReadWriteLock(true)

    //is initialized in restoreMetadata
    val max_Flushed_DocID = new AtomicLong(0)

    val nextTermID = new AtomicInteger(0)

    def getMaxDocID: Long = max_Flushed_DocID.get()

    private val atomicMAX = new LongBinaryOperator {
        override def applyAsLong(oldV: Long, newV: Long): Long = if (newV > oldV) newV else oldV
    }

    writeCacheSHLock.writeLock().lock()
    try {
        openEnvironment()
    }
    finally {
        writeCacheSHLock.writeLock().unlock()
    }

    private def openEnvironment(mapSize: Long = 0): Unit = {
        logger.info("-> openEnvironment")
        val newDir = new File(GlobalConfig.path + File.separator + "InvertedIndex")
        var createdNewFolder = false
        val onException = (message: String) => {
            val ex = new IllegalAccessException(message +
                " probably don't have execution privilege to do that")
            logger.error("!!! openEnvironment", ex)
            throw ex
        }
        if (!newDir.exists())
            if (this.storageMode == storageModeEnum.CREATE ||
                this.storageMode == storageModeEnum.READ_WRITE) {
                newDir.mkdir()
                if (!newDir.exists()) onException("Cannot create a folder")
                createdNewFolder = true
            } else {
                val ex = new FileNotFoundException("LMDB database not found")
                logger.error("!!! openEnvironment", ex)
                throw ex
            }
        else if (this.storageMode == storageModeEnum.CREATE) {
            newDir.delete()
            if (newDir.exists()) onException("Cannot delete a folder")
            newDir.mkdir()
            if (!newDir.exists()) onException("Cannot create a folder")
            createdNewFolder = true
        }
        val oldSize = new File(newDir.getPath + File.separator + "data.mdb").length()
        val increaseStep = GlobalConfig.mapSizeIncreaseStep.get()
        val overhead = if (oldSize % increaseStep == 0) 0 else 1
        val newSize: Long = if (mapSize == 0) (oldSize / increaseStep + overhead) * increaseStep
                            else mapSize
        this.environment = new Env()
        logger.debug("openEnvironment mapSize = {}", newSize)
        environment.setMapSize(newSize)
        environment.setMaxDbs(numberOfDatabases)
        //TODO: VERIFY ASSUMPTION - Constants.NORDAHEAD hurts single threaded performance
        //TODO: - improves multithreaded theoretically
        val flag = if (this.storageMode == storageModeEnum.READ_ONLY) Constants.RDONLY else 0
        environment.open(newDir.getPath, flag)
        this.postingsDB = environment.openDatabase("postings")
        this.metadataDB = environment.openDatabase("metadata")
        this.documentsDB = environment.openDatabase("documents")
        this.numberOfDeletedDB = environment.openDatabase("numberOfDeletedByBlock")
        this.roaringBitmapsDB = environment.openDatabase("roaringBitmaps")
        this.partialFlushWAL_DB = environment.openDatabase("partialFlushWAL_DB")
        this.term2ID_DB = environment.openDatabase("term2ID_DB")
        this.ID2Term_DB = environment.openDatabase("ID2Term_DB")

        this.indexStatistics_DB = environment.openDatabase("indexStatistics_DB")
        //reading stats from LMDB
        var tmpResult = this.indexStatistics_DB.get("totalDocsCount".getBytes)
        this.totalDocsCount = if (tmpResult == null) 0
        else ByteBuffer.wrap(tmpResult).getLong
        logger.info("openEnvironment totalDocsCount = {}", totalDocsCount)
        tmpResult = this.indexStatistics_DB.get("nextTermID".getBytes)
        val nextTermIDtmp = if (tmpResult == null) 0 else ByteBuffer.wrap(tmpResult).getInt
        this.nextTermID.set(nextTermIDtmp)
        logger.info("openEnvironment nextTermID = {}", nextTermIDtmp)
        if (createdNewFolder) {
            this.indexStatistics_DB.put("watermark".getBytes, watermark.getBytes())
            logger.info("openEnvironment new watermark = {}", watermark)
        } else {
            val tmp = this.indexStatistics_DB.get("watermark".getBytes)
            if (tmp != null) {
                val originalWatermark = new String(tmp)
                logger.info("openEnvironment original watermark = {}", originalWatermark)
                if (originalWatermark != watermark) {
                    val ex = new IllegalArgumentException("Reopening the index is only allowed with same" +
                        "measure and analyzer as it was created with. " +
                        s"Original watermark was: $originalWatermark" +
                        s"New watermark is: $watermark")
                    logger.error("!!! openEnvironment", ex)
                    throw ex
                }
            } else {
                this.indexStatistics_DB.put("watermark".getBytes, watermark.getBytes())
                logger.info("openEnvironment environment without watermark updated to = {}", watermark)
            }
        }
        logger.info("openEnvironment ->")
    }

    private def closeEnvironment() = {
        logger.info("-> closeEnvironment")
        this.metadataDB.close()
        this.postingsDB.close()
        this.documentsDB.close()
        this.roaringBitmapsDB.close()
        this.partialFlushWAL_DB.close()
        this.environment.close()
        this.term2ID_DB.close()
        this.ID2Term_DB.close()
        this.indexStatistics_DB.close()
        logger.info("closeEnvironment ->")
    }


    private var totalDocsCount = 0l

    def getTotalDocsCount: Long = totalDocsCount

    //GlobalLexicon section
    val termIDLock = new ReentrantLock()

    def getTermID(term: String): Int = {
        //checking write cache
        var result = writeCache.newTerms.getOrElse(term, -1)
        if (result == -1) {
            //checking LMDB
            val key = term.getBytes
            var termIDArr = term2ID_DB.get(key)
            if (termIDArr == null) {
                termIDLock.lock()
                //Double checked locking
                try {
                    //checking write cache
                    result = writeCache.newTerms.getOrElse(term, -1)
                    if (result == -1) {
                        //checking LMDB
                        termIDArr = term2ID_DB.get(term.getBytes)
                        if (termIDArr != null)
                            result = ByteBuffer.wrap(termIDArr).getInt
                        else {
                            result = nextTermID.getAndIncrement()
                            writeCache.newTerms.put(term, result)
                        }
                    }
                } finally {
                    termIDLock.unlock()
                }
            } else result = ByteBuffer.wrap(termIDArr).getInt
        }
        logger.trace("getTermID term = {} ID = {}", term, result)
        return result
    }

    def getTerm(id: Int): String = {
        val key = ByteBuffer.allocate(4).putInt(id).array()
        val result = ID2Term_DB.get(key)
        logger.info("getTerm id = {} term = {}", id, result)
        if (result == null) return null
        else new String(result)
    }

    /* doesn't perform flush*/
    def close() = {
        logger.info("-> close")
        closeEnvironment()
        logger.info("close ->")
    }

    def restoreMetadata(hook: RestoreMetadataHook): Unit = {
        logger.info("restoreMetadata ->")
        //removing garbage from partially written data, from large compactions
        val writeTx = this.environment.createWriteTransaction()
        try {
            val cursor = this.partialFlushWAL_DB.bufferCursor(writeTx)
            try {
                if (cursor.first()) {
                    do {
                        val data = cursor.valBytes()
                        val buffer = ByteBuffer.wrap(data)
                        val partOfFailedWrite = PartialFlushWAL.deserialize(buffer, data.length)
                        logger.warn("restoreMetadata -- Garbage from aborted/failed partial flush", partOfFailedWrite)
                        partOfFailedWrite.blockKeys.foreach(key => {
                            //TODO: FlyWeight can be used here, compare with ZeroCopy DirectBuffer
                            val keyArr = key.serialize
                            try {
                                this.postingsDB.delete(writeTx, keyArr)
                                this.metadataDB.delete(writeTx, keyArr)
                            } catch {
                                //shouldn't happen at all due to transactional flush implementation
                                //log level would be Error
                                case e: LMDBException => logger.error("!!! restoreMetadata", e)
                            }
                        })
                        cursor.delete()
                    } while (cursor.next())
                }
            } finally {
                cursor.close()
            }
        } catch {
            case e: Exception => logger.error("!!! restoreMetadata", e)
        } finally {
            writeTx.close()
        }
        val tx = this.environment.createReadTransaction()
        try {
            var cursor = this.metadataDB.bufferCursor(tx)
            try {
                if (cursor.first()) {
                    do {
                        val key = BlockKey.deserialize(cursor.keyBytes())
                        val data = cursor.valBytes()
                        max_Flushed_DocID.accumulateAndGet(key.maxDocID, atomicMAX)
                        hook.processMetadata(key, ByteBuffer.wrap(data))
                        logger.debug("restoreMetadata -- processMetadata for block {}", key)

                    } while (cursor.next())
                }
            } finally {
                cursor.close()
            }
            cursor = this.numberOfDeletedDB.bufferCursor(tx)
            try {
                if (cursor.first()) {
                    do {
                        val key = BlockKey.deserialize(cursor.keyBytes())
                        val nDeleted = ByteBuffer.wrap(cursor.valBytes()).getInt
                        hook.processNumDeleted(key, nDeleted)
                        logger.debug("restoreMetadata -- processNumDeleted for block {}", key)
                    } while (cursor.next())
                }
            } finally {
                cursor.close()
            }
            cursor = this.roaringBitmapsDB.bufferCursor(tx)
            try {
                if (cursor.first()) {
                    do {
                        val keyBuf = ByteBuffer.wrap(cursor.keyBytes())
                        val data = cursor.valBytes()
                        val key = keyBuf.getLong
                        hook.processBitSetSegment(BitSetSegmentSerialized(key, data))
                        logger.debug("restoreMetadata -- processBitSetSegment for block {}", key)
                    } while (cursor.next())
                }
            } finally {
                cursor.close()
            }
        } catch {
            case ex: Exception => logger.error("!!! restoreMetadata", ex)
        } finally {
            tx.close()
        }
        logger.info("restoreMetadata ->")
    }

    //=======================================================================
    //=========== BLOCKS MANAGEMENT =========================================
    //=======================================================================
    class SnapshotReader(readTx: Transaction,
                         _writeCache: WriteCache,
                         _dataBeingFlushed: WriteCache) extends ISnapshotReader {
        // search by exact key only
        def getBlock(blockKey: BlockKey): BlockDataSerialized = {
            logger.debug("SnapshotReader.getBlock blockKey = {}", blockKey)
            var ret: BlockDataSerialized = null
            if (max_Flushed_DocID.get() < blockKey.maxDocID /*&& !flushLock.isWriteLocked*/ ) {
                ret = _writeCache.addedBlocks.get(blockKey)
                if (ret == null && _dataBeingFlushed != null)
                    ret = _dataBeingFlushed.addedBlocks.get(blockKey)

            }
            if (ret == null) {
                //if not found - go to LMDB
                val key = blockKey.serialize
                ret = postingsDB.get(readTx, key)
                if (ret == null) {
                    val ex = new IllegalStateException("required block is absent")
                    logger.error("!!! SnapshotReader.getBlock", ex)
                    throw ex
                } else logger.debug("SnapshotReader.getBlock found in LMDB")
            } else logger.debug("SnapshotReader.getBlock found in cache")
            return ret
        }

        //TODO: test order, add maxMeasureValue to key as last part
        //TODO: - so we don't need to read block to see it
        /*returns actual block maxDocID and block*/
        def seekBlock(termID: Int, docID: Long): (Long, BlockDataSerialized) = {
            logger.debug("SnapshotReader.seekBlock termID = {}, docID = {}", termID, docID)
            val blockKey = BlockKey(termID, docID, 0)
            var ret: BlockDataSerialized = null
            var actualBlockKey: BlockKey = null
            if (max_Flushed_DocID.get() < docID /*&& !flushLock.isWriteLocked*/ ) {
                // first check older version of writeCache,
                // if it has block that might contain docID
                // - then prefer it over one from newer writeCache
                if (_dataBeingFlushed != null) {
                    if (_writeCache.addedBlocks.lastKey().maxDocID >= docID) {
                        val tmp = _writeCache.addedBlocks.tailMap(blockKey)
                        actualBlockKey = tmp.head._1
                        if (actualBlockKey.termID == termID)
                            ret = tmp.head._2
                    }
                }
                if (ret == null) {
                    if (_dataBeingFlushed != null &&
                        _dataBeingFlushed.addedBlocks.lastKey().maxDocID >= docID) {
                        val tmp = _dataBeingFlushed.addedBlocks.tailMap(blockKey)
                        actualBlockKey = tmp.head._1
                        if (actualBlockKey.termID == termID)
                            ret = tmp.head._2
                    }
                }
            }
            if (ret == null) {
                val cursor = postingsDB.bufferCursor(readTx)
                val rc = cursor.seekRange(blockKey.serialize)
                if (rc) {
                    actualBlockKey = BlockKey.deserialize(cursor.keyBytes())
                    if (actualBlockKey.termID == termID)
                        ret = cursor.valBytes()
                }
            } else logger.debug("SnapshotReader.seekBlock found in cache")
            if (ret != null) {
                logger.debug("SnapshotReader.seekBlock actualBlockKey = {}", actualBlockKey.maxDocID)
                return (actualBlockKey.maxDocID, ret)
            } else {
                logger.debug("SnapshotReader.seekBlock not found")
                return (-1, null)
            }
        }

        def close() = {
            readTx.close()
            logger.debug("SnapshotReader.close ->")
        }
    }

    def getSnapshotReader: ISnapshotReader = {
        logger.debug("-> getSnapshotReader")
        val tx = this.environment.createReadTransaction()
        val ret = new SnapshotReader(tx, writeCache, dataBeingFlushed)
        logger.debug("getSnapshotReader ->")
        return ret
    }

    /*override def getBlockBuffer(blockKey: BlockKey): DirectBuffer = {
        //TODO: check segmentsCache - or probably use in BlockReader ???
        //check writeCache
        if(max_Flushed_DocID.get() < blockKey.maxDocID /*&& !flushLock.isWriteLocked*/) {
            //TODO: verify assumption - we can safely read from writeCache
            //TODO: because GC strongReferences (should be)/are threadsafe
            var termWriteCache = writeCache.terms.get(blockKey.termID)
            var blockWC: BlockWriteCache = null
            if(termWriteCache != null)
                blockWC = termWriteCache.addedBlocks.get(blockKey.smallKey)
            if(blockWC == null) {
                termWriteCache = dataBeingFlushed.terms.get(blockKey.termID)
                if(termWriteCache != null)
                    blockWC = termWriteCache.addedBlocks.get(blockKey.smallKey)
            }
            if (blockWC != null)
                return new DirectBuffer(blockWC.blockData)
        }
        //if not found - go to LMDB
        val key = blockKey.serialize
        val keyBuffer = new DirectBuffer(key)
        val ret = new DirectBuffer()
        val rc = this.postingsDB.get(keyBuffer,ret)
        if(rc != 0)
            throw new IllegalStateException(s"required block is absent LMDB rc = $rc")
        else return ret
    }*/

    /*to only save block pass null for meta in TreeMetadata*/
    override def saveBlock(key: BlockKey,
                           block: BlockDataSerialized,
                           metadata: BlockMetadataSerialized): Unit = {
        logger.debug("saveBlock key = {}", key)
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        try {
            writeCache.addedMeta.put(key, metadata)
            writeCache.addedBlocks.put(key, block)
        } finally {
            sharedLock.unlock()
        }
    }


    /*for defragmenter only - doesn't change max_Flushed_DocID */
    override def replaceBlocks(deletedBlocks: Seq[BlockKey],
                               blocks: Seq[BlockCache]): Unit = {
        if (logger.isDebugEnabled)
            logger.debug("saveBlock deletedBlocks = {}, newKeys = {}", Array(deletedBlocks, blocks.map(_.key)))
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        try {
            deletedBlocks.foreach(x => {
                if (writeCache.addedBlocks.containsKey(x))
                    writeCache.addedBlocks.remove(x)
                else writeCache.deletedBlocks.remove(x)
            })
            for (newBlock <- blocks) {
                writeCache.addedMeta.put(newBlock.key, newBlock.metadata)
                writeCache.addedBlocks.put(newBlock.key, newBlock.block)
            }
        } finally {
            sharedLock.unlock()
        }
    }

    //============= HANDLING PartialFlushWAL =============================
    override def writePartialFlushWAL(data: PartialFlushWAL): Array[Byte] = {
        logger.debug("-> writePartialFlushWAL")
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        var key: Array[Byte] = null
        try {
            val dataBuffer = ByteBuffer.allocate(data.bytesRequired)
            data.serialize(dataBuffer)
            //TODO: test these two options
            key = data.blockKeys.last.serialize //java.util.UUID.randomUUID.toString.getBytes()
            writeCache.partialFlushInfosToAdd.put(key, dataBuffer.array())
        } catch {
            case ex: Exception => logger.error("writePartialFlushWAL", ex)
        }
        finally {
            sharedLock.unlock()
        }
        return key
    }

    override def deletePartialFlushWAL(key: Array[Byte]) = {
        logger.debug("-> deletePartialFlushWAL")
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        writeCache.partialFlushInfosToDELETE.add(key)
        sharedLock.unlock()
    }

    //=======================================================================
    //======== FLUSHING WRITE CACHE CONTENTS TO DISK ================================
    //=======================================================================
    override def flush(lastDocIDAssigned: Long,
                       newTotalDocsCount: Long,
                       storageLock: WriteLock,
                       metadataFlush: MetadataToFlush) = {
        logger.info("-> flush lastDocIDAssigned = {}", lastDocIDAssigned)
        val exclusiveLock = writeCacheSHLock.writeLock()
        exclusiveLock.lock()
        try {
            this.totalDocsCount = newTotalDocsCount
            if (storageLock != null && storageLock.isHeldByCurrentThread)
                storageLock.unlock()
            // Rotation
            dataBeingFlushed = writeCache
            val dbf = dataBeingFlushed
            writeCache = new WriteCache()
            exclusiveLock.unlock()
            actualFlush(dataBeingFlushed,
                metadataFlush,
                this.totalDocsCount,
                this.nextTermID.get())
            // unreferencing written data, to free it's space earlier
            // if it is still the same object
            dataBeingFlushed.synchronized {
                if (dataBeingFlushed == dbf)
                    dataBeingFlushed = null
            }
            max_Flushed_DocID.accumulateAndGet(lastDocIDAssigned, atomicMAX)
        } finally {
            if (exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
        logger.info("flush ->")
    }

    private val flushInProgressLock = new ReentrantLock()

    private def actualFlush(data: WriteCache,
                            metadataFlush: MetadataToFlush,
                            newTotalDocsCount: Long,
                            nextTermID: Int,
                            attempt: Byte = 1): Unit = {
        logger.debug("flush newTotalDocsCount = {}, nextTermID = {}, attempt = {}",
            Array(newTotalDocsCount, nextTermID, attempt))
        flushInProgressLock.lock()
        val tx = this.environment.createWriteTransaction()
        var isAborted = false
        val abort = (e: Exception) => {
            isAborted = true
            tx.abort()
            tx.close()
            logger.error("!!! actualFlush transaction aborted", e)
        }
        //TODO: rewrite retries to use tx.reset instead of creating new one
        try {
            val tmp8 = ByteBuffer.allocate(8)
            val tmp4 = ByteBuffer.allocate(4)
            val tmpBlockKey = ByteBuffer.allocate(BlockKey.sizeInBytes)
            def resetBuf = (buf: ByteBuffer) => {
                buf.position(0)
                buf
            }
            def buffer8 = resetBuf(tmp8)
            def buffer4 = resetBuf(tmp4)
            def bufferBlockKey = resetBuf(tmpBlockKey)

            var tmpValue = buffer8.putLong(newTotalDocsCount).array()
            this.indexStatistics_DB.put(tx, "totalDocsCount".getBytes, tmpValue)
            tmpValue = buffer4.putInt(nextTermID).array()
            this.indexStatistics_DB.put(tx, "nextTermID".getBytes, tmpValue)

            for (blockKey <- data.deletedBlocks.iterator) {
                logger.trace("actualFlush -- deleting = {}", blockKey)
                val key = blockKey.serialize(bufferBlockKey)
                this.postingsDB.delete(tx, key)
                this.metadataDB.delete(tx, key)
                this.numberOfDeletedDB.delete(tx, key)
            }
            for ((blockKey, metadataSerialized) <- data.addedMeta) {
                logger.trace("actualFlush -- adding Metadata = {}", blockKey)
                this.metadataDB.put(tx, blockKey.serialize(bufferBlockKey), metadataSerialized)
            }
            for ((blockKey, blockData) <- data.addedBlocks) {
                logger.trace("actualFlush -- adding blocks = {}", blockKey)
                this.postingsDB.put(tx, blockKey.serialize(bufferBlockKey), blockData)
            }
            for ((term, newID) <- data.newTerms) {
                logger.trace("actualFlush -- adding term = {}, ID = {}", term, newID)
                val key = term.getBytes
                val value = buffer4.putInt(newID).array()
                term2ID_DB.put(key, value)
                ID2Term_DB.put(value, key)
            }
            for (key <- data.partialFlushInfosToDELETE.iterator()) {
                logger.trace("actualFlush -- deleting partialFlushInfo flushID = {}", key)
                this.partialFlushWAL_DB.delete(tx, key)
            }
            for ((key, value) <- data.partialFlushInfosToAdd) {
                logger.trace("actualFlush -- adding partialFlushInfo flushID = {}", key)
                this.partialFlushWAL_DB.put(tx, key, value)
            }
            for (docID <- data.docs.deletedDocuments.iterator()) {
                logger.trace("actualFlush -- deleting document docID = {}", docID)
                val key = buffer8.putLong(docID).array()
                this.documentsDB.delete(tx, key)
            }
            for (doc <- data.docs.addedDocuments) {
                logger.info("actualFlush -- adding document docID = {} body = {}", doc._1, doc._2.length)
                val key = buffer8.putLong(doc._1).array()
                this.documentsDB.put(tx, key, doc._2)
            }
            for (delInfo <- metadataFlush.deletionInfo) {
                logger.trace("actualFlush -- deletionInfo = {}", delInfo._1)
                val key = delInfo._1.serialize(bufferBlockKey)
                val value = buffer4.putInt(delInfo._2).array()
                this.numberOfDeletedDB.put(tx, key, value)
            }
            for (segment <- metadataFlush.bitSetSegments) {
                logger.trace("actualFlush -- bitSetSegment key = {}", segment.key)
                val key = buffer8.putLong(segment.key).array()
                this.roaringBitmapsDB.put(tx, key, segment.data)
            }
        } catch {
            case e: LMDBException => {
                try {
                    val timestamp = System.currentTimeMillis()
                    abort(e)
                    //TODO: make 2 - configurable parameter
                    if (e.getErrorCode == LMDBException.MAP_FULL && attempt < 2) {
                        isAborted = false
                        //close, increase mapSize, open
                        val currentSize = this.environment.info().getMapSize
                        val increaseStep = GlobalConfig.mapSizeIncreaseStep.get()
                        val newSize = currentSize + increaseStep
                        closeEnvironment()
                        openEnvironment(newSize)
                        //secondAttempt
                        logger.warn("actualFlush -- MAP_FULL oldSize = {} newSize = {}",currentSize,newSize)
                        actualFlush(data,
                            metadataFlush,
                            newTotalDocsCount: Long,
                            nextTermID: Int,
                            (attempt + 1).asInstanceOf[Byte])
                    } else {
                        val ex = new IllegalArgumentException("actualFlush mapSizeIncreaseStep is likely too small", e)
                        logger.error("!!! actualFlush cannot handle ", ex)
                        throw ex
                    }
                } catch {
                    case ex: Exception => abort(e)
                }
            }
            case e: Exception => abort(e)
        } finally {
            if (!isAborted) {
                tx.commit()
                tx.close()
                if (attempt > 1)
                    logger.warn(" actualFlush -- Successful retry of write transaction")
                logger.info("actualFlush successful ->")
            }
            if (flushInProgressLock.isHeldByCurrentThread)
                flushInProgressLock.unlock()
        }
    }

    //=======================================================================
    //======== DOCUMENTS MANAGEMENT =========================================
    //=======================================================================
    def addDocument(docID: Long, doc: DocumentSerialized): Unit = {
        logger.debug("addDocument docID = {}, doc = {}", docID, doc)
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        writeCache.docs.addedDocuments.put(docID, doc)
        sharedLock.unlock()
    }

    def getDocument(docID: Long): Option[DocumentSerialized] = {
        logger.debug("addDocument docID = {}", docID)
        val key = ByteBuffer.allocate(8)
        key.putLong(docID)
        val doc = this.documentsDB.get(key.array())
        if (doc != null) {
            logger.debug("getDocument document found")
            return Some(doc)
        } else {
            logger.debug("getDocument document not found")
            return None
        }
    }

    def removeDocument(docID: Long): Unit = {
        logger.debug("removeDocument docID = {}", docID)
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        writeCache.docs.deletedDocuments.add(docID)
        sharedLock.unlock()
    }
}
