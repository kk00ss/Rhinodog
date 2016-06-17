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

import rhinodog.Core.Definitions._
import BaseTraits._
import Caching._
import Configuration._

import scala.collection.JavaConversions.{asScalaIterator, mapAsScalaMap}
import scala.collection.Seq

class Repository(storageMode: storageModeEnum.storageMode,
                 //full names of measure, analyzer
                 watermark: String)
    extends IRepository {

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

    val numberOfDatabases = 9

    //TODO investigate re-usability of WriteCache instances
    var writeCache = new WriteCache()
    var dataBeingFlushed: WriteCache = null

    val writeCacheSHLock = new ReentrantReadWriteLock(true)

    //is initialized in restoreMetadata
    val max_Flushed_DocID = new AtomicLong(0)

    //should be initialized from indexStatistics_DB
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
        val newDir = new File(GlobalConfig.path + File.separator + "InvertedIndex")
        if (!newDir.exists())
            if (this.storageMode == storageModeEnum.CREATE)
                newDir.mkdir()
            else throw new FileNotFoundException("LMDB database not found")
        else if(this.storageMode == storageModeEnum.CREATE) {
            newDir.delete()
            newDir.mkdir()
        }
        val oldSize = new File(newDir.getPath + File.separator + "data.mdb").length()
        val overhead = if (oldSize % GlobalConfig.mapSizeIncreaseStep == 0) 0 else 1
        val newSize = if (mapSize == 0) {
            if (oldSize == 0) GlobalConfig.mapSizeIncreaseStep
            else GlobalConfig.mapSizeIncreaseStep * (oldSize / GlobalConfig.mapSizeIncreaseStep + overhead)
        } else mapSize
        this.environment = new Env()
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
        this.totalDocsCount = if(tmpResult == null) 0
        else ByteBuffer.wrap(tmpResult).getLong
        tmpResult = this.indexStatistics_DB.get("nextTermID".getBytes)
        val nextTermIDtmp = if(tmpResult == null) 0 else ByteBuffer.wrap(tmpResult).getInt
        this.nextTermID.set(nextTermIDtmp)
        if(this.storageMode == storageModeEnum.CREATE)
            this.indexStatistics_DB.put("watermark".getBytes,watermark.getBytes())
        else {
            val originalWatermark = new String(this.indexStatistics_DB.get("watermark".getBytes))
            if(originalWatermark != watermark)
                throw new IllegalArgumentException("Reopening the index is only allowed with same" +
                    "measure and analyzer as it was created with. " +
                    s"Original watermark was: $originalWatermark"+
                    s"New watermark is: $watermark")
        }
    }

    private def closeEnvironment() = {
        this.metadataDB.close()
        this.postingsDB.close()
        this.documentsDB.close()
        this.roaringBitmapsDB.close()
        this.partialFlushWAL_DB.close()
        this.environment.close()
        this.term2ID_DB.close()
        this.ID2Term_DB.close()
        this.indexStatistics_DB.close()
    }


    private var totalDocsCount = 0l
    def getTotalDocsCount: Long = totalDocsCount


    //GlobalLexicon section
    val termIDLock = new ReentrantLock()

    def getTermID(term: String): Int = {
        //checking write cache
        val ret1 = writeCache.newTerms.getOrElse(term, -1)
        if(ret1 != -1)
            return ret1
        //checking LMDB
        val key = term.getBytes
        //println("term - "+term)
        var termIDArr = term2ID_DB.get(key)
        if(termIDArr == null) {
            termIDLock.lock()
            //Double checked locking
            try {
                //checking write cache
                val ret1 = writeCache.newTerms.getOrElse(term, -1)
                if(ret1 != -1)
                    return ret1
                //checking LMDB
                termIDArr = term2ID_DB.get(term.getBytes)
                if(termIDArr != null) return ByteBuffer.wrap(termIDArr).getInt
                else {
                    val newID = nextTermID.getAndIncrement()
                    writeCache.newTerms.put(term,newID)
                    return newID
                }
            } finally { termIDLock.unlock() }
        } else return ByteBuffer.wrap(termIDArr).getInt
    }

    def getTerm(id: Int): String  = {
        val key = ByteBuffer.allocate(4).putInt(id).array()
        val ret = ID2Term_DB.get(key)
        if(ret == null) return  null
        else new String(ret)
    }

    /* doesn't perform flush*/
    def close() = closeEnvironment()

    def restoreMetadata(hook: RestoreMetadataHook): Unit = {
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
                        partOfFailedWrite.blockKeys.foreach(key => {
                            //TODO: FlyWeight can be used here, compare with ZeroCopy DirectBuffer
                            val keyArr = key.serialize
                            try {
                                this.postingsDB.delete(writeTx, keyArr)
                                this.metadataDB.delete(writeTx, keyArr)
                            } catch {
                                //shouldn't happen at all due to transactional flush implementation
                                //log level would be Error
                                case e: LMDBException => println(e.getMessage)
                            }
                        })
                        cursor.delete()
                    } while (cursor.next())
                }
            } finally {
                cursor.close()
            }
        } catch {
            case e: Exception => e.printStackTrace()
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
                    } while (cursor.next())
                }
            } finally {
                cursor.close()
            }
        } catch {
            case e: Exception => e.printStackTrace()
        } finally {
            tx.close()
        }
    }

    //=======================================================================
    //=========== BLOCKS MANAGEMENT =========================================
    //=======================================================================
    class SnapshotReader(readTx: Transaction,
                         _writeCache: WriteCache,
                         _dataBeingFlushed: WriteCache) extends ISnapshotReader {
        // search by exact key only
        def getBlock(blockKey: BlockKey): BlockDataSerialized = {
            //TODO: NO READ CACHE, SegmentsCaceh will contain already decoded segments
            var ret: BlockDataSerialized = null
            if (max_Flushed_DocID.get() < blockKey.maxDocID /*&& !flushLock.isWriteLocked*/ ) {
                ret = _writeCache.addedBlocks.get(blockKey)
                if (ret == null && _dataBeingFlushed != null)
                        ret = _dataBeingFlushed.addedBlocks.get(blockKey)
            } else {
                //if not found - go to LMDB
                val key = blockKey.serialize
                ret = postingsDB.get(readTx, key)
                if (ret == null)
                    throw new IllegalStateException("required block is absent")
            }
            return ret
        }

        //TODO: test order, add maxMeasureValue to key as last part
        //TODO: - so we don't need to read block to see it
        /*returns actual block maxDocID and block*/
        def seekBlock(termID: Int, docID: Long): (Long, BlockDataSerialized) = {
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
                        if(actualBlockKey.termID == termID)
                            ret = tmp.head._2
                    }
                }
                if (ret == null) {
                    if (_dataBeingFlushed != null &&
                        _dataBeingFlushed.addedBlocks.lastKey().maxDocID >= docID) {
                        val tmp = _dataBeingFlushed.addedBlocks.tailMap(blockKey)
                        actualBlockKey = tmp.head._1
                        if(actualBlockKey.termID == termID)
                            ret = tmp.head._2
                    }
                }
            } else {
                val cursor = postingsDB.bufferCursor(readTx)
                val rc = cursor.seekRange(blockKey.serialize)
                if (rc) {
                    actualBlockKey = BlockKey.deserialize(cursor.keyBytes())
                    if(actualBlockKey.termID == termID)
                        ret = cursor.valBytes()
                }
            }
            if(ret != null) return (actualBlockKey.maxDocID, ret)
            else return (-1, null)
        }

        def close() = readTx.close()
    }

    def getSnapshotReader: ISnapshotReader
    = new SnapshotReader(this.environment.createReadTransaction(), writeCache, dataBeingFlushed)

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
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        writeCache.addedMeta.put(key, metadata)
        writeCache.addedBlocks.put(key, block)
        sharedLock.unlock()
    }


    /*for defragmenter only - doesn't change max_Flushed_DocID */
    override def replaceBlocks(deletedBlocks: Seq[BlockKey],
                               blocks: Seq[BlockCache]): Unit = {
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        deletedBlocks.foreach(x => {
            if (writeCache.addedBlocks.containsKey(x))
                writeCache.addedBlocks.remove(x)
            else writeCache.deletedBlocks.remove(x)
        })
        for(newBlock <- blocks) {
            writeCache.addedMeta.put(newBlock.key, newBlock.metadata)
            writeCache.addedBlocks.put(newBlock.key, newBlock.block)
        }
        sharedLock.unlock()
    }

    //============= HANDLING PartialFlushWAL =============================
    override def writePartialFlushWAL(data: PartialFlushWAL): Array[Byte] = {
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        try {
            val dataBuffer = ByteBuffer.allocate(data.bytesRequired)
            data.serialize(dataBuffer)
            //TODO: test these two options
            val key = data.blockKeys.last.serialize //java.util.UUID.randomUUID.toString.getBytes()
            writeCache.partialFlushInfosToAdd.put(key, dataBuffer.array())
            return key
        } finally {
            sharedLock.unlock()
        }
    }

    override def deletePartialFlushWAL(key: Array[Byte]) = {
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        try {
            writeCache.partialFlushInfosToDELETE.add(key)
        } finally {
            sharedLock.unlock()
        }
    }

    //=======================================================================
    //======== FLUSHING WRITE CACHE CONTENTS TO DISK ================================
    //=======================================================================
    override def flush(lastDocIDAssigned: Long,
                       newTotalDocsCount: Long,
                       storageLock: WriteLock,
                       metadataFlush: MetadataToFlush) = {
        val exclusiveLock = writeCacheSHLock.writeLock()
        exclusiveLock.lock()
        try {
            this.totalDocsCount = newTotalDocsCount
            if (storageLock != null && storageLock.isHeldByCurrentThread) storageLock.unlock()
            // Rotation
            dataBeingFlushed = writeCache
            val dbf = dataBeingFlushed
            writeCache = new WriteCache()
            exclusiveLock.unlock()
            flush1(dataBeingFlushed, metadataFlush, this.totalDocsCount, this.nextTermID.get())
            // unreferencing written data, to free it's space earlier
            // if it is still the same object
            dataBeingFlushed.synchronized {
                if (dataBeingFlushed == dbf)
                    dataBeingFlushed = null
            }
            max_Flushed_DocID.accumulateAndGet(lastDocIDAssigned, atomicMAX)
        } finally {
            if (exclusiveLock.isHeldByCurrentThread) exclusiveLock.unlock()
        }
    }

    private val flushInProgressLock = new ReentrantLock()

    private def flush1(data: WriteCache,
                       metadataFlush: MetadataToFlush,
                       newTotalDocsCount: Long,
                       nextTermID: Int,
                       attempt: Byte = 1): Unit = {
        flushInProgressLock.lock()
        val tx = this.environment.createWriteTransaction()
        var isAborted = false
        val abort = (e: Exception) => {
            isAborted = true
            e.printStackTrace()
            tx.abort()
        }
        try {
            var tmpValue = ByteBuffer.allocate(8).putLong(newTotalDocsCount).array()
            this.indexStatistics_DB.put(tx, "totalDocsCount".getBytes, tmpValue)
            tmpValue = ByteBuffer.allocate(4).putInt(nextTermID).array()
            this.indexStatistics_DB.put(tx, "nextTermID".getBytes, tmpValue)

            for (blockKey <- data.deletedBlocks.iterator) {
                val key = blockKey.serialize
                this.postingsDB.delete(tx, key)
                this.metadataDB.delete(tx, key)
                this.numberOfDeletedDB.delete(tx, key)
            }
            for ((rootKey, metadataSerialized) <- data.addedMeta) {
                this.metadataDB.put(tx, rootKey.serialize, metadataSerialized)
            }
            for ((key, blockData) <- data.addedBlocks) {
                this.postingsDB.put(tx, key.serialize, blockData)
            }
            for((term, newID) <- data.newTerms) {
                val key = term.getBytes
                val value = ByteBuffer.allocate(4).putInt(newID).array()
                term2ID_DB.put(key, value)
                ID2Term_DB.put(value, key)
            }
            for (key <- data.partialFlushInfosToDELETE.iterator()) {
                this.partialFlushWAL_DB.delete(tx, key)
            }
            for ((key, value) <- data.partialFlushInfosToAdd) {
                this.partialFlushWAL_DB.put(tx, key, value)
            }
            for (docID <- data.docs.deletedDocuments.iterator()) {
                val key = ByteBuffer.allocate(8)
                key.putLong(docID)
                this.documentsDB.delete(tx, key.array())
            }
            for (doc <- data.docs.addedDocuments) {
                val key = ByteBuffer.allocate(8)
                key.putLong(doc._1)
                this.documentsDB.put(tx, key.array(), doc._2)
            }
            for (delInfo <- metadataFlush.deletionInfo) {
                val key = delInfo._1.serialize
                val value = ByteBuffer.allocate(4)
                value.putInt(delInfo._2)
                this.numberOfDeletedDB.put(tx, key, value.array())
            }
            for (segment <- metadataFlush.bitSetSegments) {
                val key = ByteBuffer.allocate(8)
                key.putLong(segment.key)
                this.roaringBitmapsDB.put(tx, key.array(), segment.data)
            }
        } catch {
            case e: LMDBException => {
                try {
                    val timestamp = System.currentTimeMillis()
                    abort(e)
                    println("=========== Retrying transaction " + timestamp)
                    //TODO: make 2 - configurable parameter
                    if (attempt < 2) {
                        isAborted = false
                        //close, increase mapSize, open
                        val newSize = this.environment.info().getMapSize + GlobalConfig.mapSizeIncreaseStep
                        closeEnvironment()
                        openEnvironment(newSize)
                        //secondAttempt
                        flush1(data,
                            metadataFlush,
                            newTotalDocsCount: Long,
                            nextTermID: Int,
                            (attempt + 1).asInstanceOf[Byte])
                    } else throw new IllegalArgumentException("mapSizeIncreaseStep is likely too small", e)
                    println("=========== AFTER " + timestamp)
                } catch {
                    case ex: Exception => {
                        println("=========== CANNOT HANDLE LMDB EXCEPTION")
                        abort(e)
                    }
                }
            }
            case e: Exception => abort(e)
        } finally {
            if (!isAborted)
                tx.commit()
            tx.close()
            if(attempt > 1)
                println("Successful retry of write transaction")
            if(flushInProgressLock.isHeldByCurrentThread)
                flushInProgressLock.unlock()
        }
    }

    //=======================================================================
    //======== DOCUMENTS MANAGEMENT =========================================
    //=======================================================================
    def addDocument(docID: Long, doc: DocumentSerialized): Unit = {
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        writeCache.docs.addedDocuments.put(docID, doc)
        sharedLock.unlock()
    }

    def getDocument(docID: Long): Option[DocumentSerialized] = {
        val key = ByteBuffer.allocate(8)
        key.putLong(docID)
        val doc = this.documentsDB.get(key.array())
        if (doc != null) return Some(doc)
        else return None
    }

    def removeDocument(docID: Long): Unit = {
        val sharedLock = writeCacheSHLock.readLock()
        sharedLock.lock()
        writeCache.docs.deletedDocuments.add(docID)
        sharedLock.unlock()
    }
}
