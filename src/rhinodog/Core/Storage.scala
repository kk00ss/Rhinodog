package rhinodog.Core

import java.nio.file.Paths
import java.util.{TimerTask, Timer}
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.Seq
import scala.collection.JavaConversions.mapAsScalaMap
import scala.collection.JavaConversions.iterableAsScalaIterable

import Utils._
import Definitions._
import BaseTraits._
import Configuration._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class Storage
(val storageConfig: StorageConfig) {
    import storageConfig._
    import mainComponents._

    private val termWriters = new ConcurrentHashMap[Int, TermWriter]()
    private val termWriterConfigTemplage = new TermWriterConfig(0, mainComponents,
                                                                globalConfig.targetBlockSize,
                                                                globalConfig.maxNodeUnderflow)
    @volatile
    private var isOpen = true

    def getTermWriter(termID: Int): TermWriter = {
        if (termWriters.contains(termID))
            return termWriters.get(termID)
        else {
            val conf = termWriterConfigTemplage.copy(termID)
            val newInstance = new TermWriter(conf)
            termWriters.putIfAbsent(termID, newInstance)
            return termWriters.get(termID)
        }
    }

    private lazy val docIDCounter = new AtomicLong(repository.getMaxDocID)
    private val modifyOrFlushLock = new ReentrantReadWriteLock()
    private val compactionsOrCloseLock = new ReentrantReadWriteLock()
    private val docSerializer = new DocumentSerializer (measureSerializer)

    def withSharedLock(func: ()=> Unit) = {
        val sharedLock = modifyOrFlushLock.readLock()
        sharedLock.lock()
        try {
            func()
        } finally { sharedLock.unlock() }
    }

    //========================= Merge Scheduling ===================================================
    private val scheduledCompactions = new ConcurrentLinkedDeque[CompactionJobInterface]
    private val compactionsRunning = new AtomicInteger(0)

    def addCompactionJob(job: CompactionJobInterface): Unit
    = if(isOpen) scheduledCompactions.add(job)

    val fsRoot = Paths.get(".").toAbsolutePath.getRoot.toFile

    val timer = new Timer()
    timer.schedule(new TimerTask { override def run() = {
        //update free space ratio
//        val totalSpace = fsRoot.getTotalSpace
//        val freeSpace = fsRoot.getFreeSpace
//        val freeSpaceRatio = (freeSpace + 0f)/totalSpace
//        storageConfig.updateFreeSpaceRatio(freeSpaceRatio)
        if(operatingSystemMXBean.getSystemCpuLoad <= globalConfig.merges_CpuLoadThreshold ) {
            while(isOpen && scheduledCompactions.nonEmpty &&
                compactionsRunning.get() < globalConfig.maxConcurrentCompactions) {
                //starting new compaction
                Future {
                    compactionsOrCloseLock.readLock().lock()
                    compactionsRunning.incrementAndGet()
                    try {
                        if(isOpen) {
                            val compactionToRun = scheduledCompactions.pollLast()
                            //compute merges
                            val saveChangesHook = compactionToRun.computeChanges()
                            //save results of all the merges
                            val tw = getTermWriter(compactionToRun.termID)
                            saveChangesHook(withSharedLock, tw.withLock)
                        }
                    } finally {
                        compactionsOrCloseLock.readLock().unlock()
                        compactionsRunning.decrementAndGet()
                    }
                }
            }
        }
    }}, globalConfig.merges_QueueCheckInterval,
        globalConfig.merges_QueueCheckInterval)

    private val nSmallFlushesFromLastLarge = new AtomicInteger(0)

    timer.schedule(new TimerTask {
        override def run(): Unit = if(isOpen) {
            smallFlush()
            val tmp = nSmallFlushesFromLastLarge.incrementAndGet()
            if(tmp == globalConfig.largeFlushInterval) {
                largeFlush()
                nSmallFlushesFromLastLarge.set(0)
            }
        }
    }, globalConfig.smallFlushInterval,
       globalConfig.smallFlushInterval)



    val operatingSystemMXBean = management.ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    //==============================================================================================

    def totalDocs: Long = _totalDocs.get()

    //TODO: restore after reloading from Repository
    private val _totalDocs = new AtomicLong(0)

    def addDocument(docInfo: AnalyzedDocument): Long = {
        val sharedLock = modifyOrFlushLock.readLock()
        sharedLock.lock()
        var currentDocID = -1l
        try {
            currentDocID = docIDCounter.incrementAndGet()
            val document = AnalyzedDocument(docInfo.text, docInfo.terms)
            val operations = document.terms.map(td => (new DocPosting(currentDocID, td.measure),
                                                        getTermWriter(td.termID))).toBuffer
            var operationTimeout = 1
            while (operations.nonEmpty) {
                for (op <- operations) {
                    val ret = op._2.tryAdd(op._1, operationTimeout)
                    if (ret)
                        operations -= op
                }
                operationTimeout *= 2
            }
            val docSerialized = docSerializer.serialize(document)
            repository.addDocument(currentDocID, docSerialized)
            _totalDocs.incrementAndGet()
            return currentDocID
        } finally {
            sharedLock.unlock()
        }
        return currentDocID
    }

    def removeDocument(documentID: Long) = {
        val sharedLock = modifyOrFlushLock.readLock()
        sharedLock.lock()
        try {
            if (documentID > docIDCounter.get())
                throw new IllegalArgumentException("no such document exist")
            if (!metadataManager.isDeleted(documentID)) {
                val docBody = repository.getDocument(documentID)
                if (docBody.isEmpty)
                    throw new IllegalArgumentException("no such document exist")
                val document = docSerializer.deserialize(docBody.get)
                metadataManager.markDeleted(documentID)
                _totalDocs.decrementAndGet()
                val operations = document.terms
                    .map(td => (td.termID, td.measure, getTermWriter(td.termID)))
                    .toBuffer
                var operationTimeout = 1
                while (operations.nonEmpty) {
                    for (op <- operations) {
                        val ret = op._3.tryWithLock(
                            () => metadataManager.deleteFromTerm(op._1, op._2, documentID),
                            operationTimeout)
                        if (ret)
                            operations -= op
                    }
                    operationTimeout *= 2
                }
            }
        } finally {
            sharedLock.unlock()
        }
    }

    def flush() = {
        val exclusiveLock = modifyOrFlushLock.writeLock()
        exclusiveLock.lock()
        try {
            smallFlush()
            largeFlush()
        } finally { exclusiveLock.unlock() }
    }

    def smallFlush() = {
        val exclusiveLock = modifyOrFlushLock.writeLock()
        val wasAlreadyLocked = exclusiveLock.isHeldByCurrentThread
        if(!wasAlreadyLocked)
            exclusiveLock.lock()
        try {
            termWriters.foreach(_._2.smallFlush())
        } finally {
            if(!wasAlreadyLocked && exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def largeFlush(): Unit = {
        val exclusiveLock = modifyOrFlushLock.writeLock()
        val wasAlreadyLocked = exclusiveLock.isHeldByCurrentThread
        if(!wasAlreadyLocked)
            exclusiveLock.lock()
        try {
            termWriters.foreach(_._2.largeFlush())
            val metadataToFlush = metadataManager.flush
            repository.flush(docIDCounter.get(), exclusiveLock, metadataToFlush)
        } finally {
            if(!wasAlreadyLocked  && exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def close() = {
        isOpen = false
        compactionsOrCloseLock.writeLock().lock()
        try {
            flush()
            repository.close()
            timer.cancel()
        } finally { compactionsOrCloseLock.writeLock().unlock() }
    }
}
