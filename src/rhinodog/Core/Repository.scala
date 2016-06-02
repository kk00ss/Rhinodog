package rhinodog.Core

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock
import java.util.function.LongBinaryOperator

import org.fusesource.lmdbjni._

import rhinodog.Core.Definitions._
import rhinodog.Core.Definitions.BaseTraits._
import Caching._
import Configuration._
import Utils._

import scala.collection.JavaConversions.{asScalaIterator, mapAsScalaMap}
import scala.collection.Seq

class Repository(globalConfig: GlobalConfig)
    extends RepositoryBase {

    private var environment: Env = null
    private var postingsDB: Database = null
    private var metadataDB: Database = null
    private var documentsDB: Database = null
    private var numberOfDeletedDB: Database = null
    private var roaringBitmapsDB: Database = null
    private var partialFlushWAL_DB: Database = null

    private var term2ID_DB: Database = null
    private var ID2Term_DB: Database = null

    val numberOfDatabases = 8

    //TODO investigate reusability of WriteCache instances
    var writeCache = new WriteCache()
    var dataBeingFlushed: WriteCache = null

    val writeCacheSHLock = new ReentrantReadWriteLock(true)

    //is initialized in restoreMetadata
    val max_Flushed_DocID = new AtomicLong(0)

    def getMaxDocID: Long = max_Flushed_DocID.get()

    private val atomicMAX = new LongBinaryOperator {
        override def applyAsLong(oldV: Long, newV: Long): Long = if (newV > oldV) newV else oldV
    }

    init()

    def init(): Unit = {
        val exclusiveLock = writeCacheSHLock.writeLock()
        exclusiveLock.lock()
        openEnvironment()
        exclusiveLock.unlock()
    }

    private def openEnvironment(mapSize: Long = 0): Unit = {
        val newDir = new File(globalConfig.path + File.separator + "subEnvironment_")
        if (!newDir.exists())
            if (globalConfig.storageMode == storageModeEnum.CREATE)
                newDir.mkdir()
            else throw new FileNotFoundException("LMDB database not found")
        val oldSize = new File(newDir.getPath + File.separator + "data.mdb").length()
        val overhead = if (oldSize % globalConfig.mapSizeIncreaseStep == 0) 0 else 1
        val newSize = if (mapSize == 0) {
            if (oldSize == 0) globalConfig.mapSizeIncreaseStep
            else globalConfig.mapSizeIncreaseStep * (oldSize / globalConfig.mapSizeIncreaseStep + overhead)
        } else mapSize
        this.environment = new Env()
        environment.setMapSize(newSize)
        environment.setMaxDbs(numberOfDatabases)
        //TODO: VERIFY ASSUMPTION - Constants.NORDAHEAD hurts single threaded performance
        // - improves multithreaded theoretically
        val flag = if (globalConfig.storageMode == storageModeEnum.WRITEnREAD) Constants.RDONLY else 0
        environment.open(newDir.getPath, flag)
        this.postingsDB = environment.openDatabase("postings")
        this.metadataDB = environment.openDatabase("metadata")
        this.documentsDB = environment.openDatabase("documents")
        this.numberOfDeletedDB = environment.openDatabase("numberOfDeletedByBlock")
        this.roaringBitmapsDB = environment.openDatabase("roaringBitmaps")
        this.partialFlushWAL_DB = environment.openDatabase("partialFlushWAL_DB")
        this.term2ID_DB = environment.openDatabase("term2ID_DB")
        this.ID2Term_DB = environment.openDatabase("ID2Term_DB")
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
    }


    //GlobalLexicon section
    val nextTermID = new AtomicInteger(0)
    val termIDLock = new ReentrantLock()

    def getTermID(term: String): Int = {
        val key = term.getBytes
        //checking write cache
        val ret1 = writeCache.newTerms.getOrElse(term, -1)
        if(ret1 != -1)
            return ret1
        //checking LMDB
        var termID = term2ID_DB.get(key)
        if(termID != null) return ByteBuffer.wrap(termID).getInt
        else {
            termIDLock.lock()
            //Double checked locking
            try {
                //checking write cache
                val ret1 = writeCache.newTerms.getOrElse(term, -1)
                if(ret1 != -1)
                    return ret1
                //checking LMDB
                termID = term2ID_DB.get(term.getBytes)
                if(termID != null) return ByteBuffer.wrap(termID).getInt
                else {
                    val newID = nextTermID.getAndIncrement()
                    writeCache.newTerms.put(term,newID)
                    return newID
                }
            } finally { termIDLock.unlock() }
        }
    }
    //doesn't work with writeCache
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
                         _dataBeingFlushed: WriteCache) extends SnapshotReaderInterface {
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

    def getSnapshotReader: SnapshotReaderInterface
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
                       storageLock: WriteLock,
                       metadataFlush: MetadataToFlush) = {
        val exclusiveLock = writeCacheSHLock.writeLock()
        exclusiveLock.lock()
        try {
            if (storageLock != null && storageLock.isHeldByCurrentThread) storageLock.unlock()
            // Rotation
            dataBeingFlushed = writeCache
            val dbf = dataBeingFlushed
            writeCache = new WriteCache()
            exclusiveLock.unlock()
            flush1(dataBeingFlushed, metadataFlush)
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

    private def flush1(data: WriteCache,
                       metadataFlush: MetadataToFlush,
                       attempt: Byte = 1): Unit = {
        val tx = this.environment.createWriteTransaction()
        val postingsDB = this.postingsDB
        val metaDB = this.metadataDB
        val docsDB = this.documentsDB
        val nDeletedDB = this.numberOfDeletedDB
        val roaringDB = this.roaringBitmapsDB
        var isAborted = false
        val abort = (e: Exception) => {
            isAborted = true
            e.printStackTrace()
            tx.abort()
        }
        try {
            for (blockKey <- data.deletedBlocks.iterator) {
                val key = blockKey.serialize
                postingsDB.delete(tx, key)
                metaDB.delete(tx, key)
                nDeletedDB.delete(tx, key)
            }
            for ((rootKey, metadataSerialized) <- data.addedMeta) {
                metaDB.put(tx, rootKey.serialize, metadataSerialized)
            }
            for ((key, blockData) <- data.addedBlocks) {
                postingsDB.put(tx, key.serialize, blockData)
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
                docsDB.delete(tx, key.array())
            }
            for (doc <- data.docs.addedDocuments) {
                val key = ByteBuffer.allocate(8)
                key.putLong(doc._1)
                docsDB.put(tx, key.array(), doc._2)
            }
            for (delInfo <- metadataFlush.deletionInfo) {
                val key = delInfo._1.serialize
                val value = ByteBuffer.allocate(4)
                value.putInt(delInfo._2)
                nDeletedDB.put(tx, key, value.array())
            }
            for (segment <- metadataFlush.bitSetSegments) {
                val key = ByteBuffer.allocate(8)
                key.putLong(segment.key)
                roaringDB.put(tx, key.array(), segment.data)
            }
        } catch {
            case e: LMDBException => {
                try {
                    val timestamp = System.currentTimeMillis()
                    println("=========== Exception is handled correctly " + timestamp)
                    abort(e)
                    if (attempt < 2) {
                        isAborted = false
                        //close, increase mapSize, open
                        val newSize = this.environment.info().getMapSize + globalConfig.mapSizeIncreaseStep
                        closeEnvironment()
                        openEnvironment(newSize)
                        //secondAttempt
                        flush1(data,
                            metadataFlush,
                            (attempt + 1).asInstanceOf[Byte])
                    } else throw new IllegalArgumentException("mapSizeIncreaseStep is likely too small")
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
