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

import java.nio.file.Paths
import java.util.{TimerTask, Timer}
import java.util.concurrent._
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.slf4j.LoggerFactory

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
(val mainComponents: MainComponents) {
    private val logger = LoggerFactory.getLogger(this.getClass)
    import mainComponents._

    private val termWriters = new ConcurrentHashMap[Int, TermWriter]()
    private val termWriterConfigTemplage = new TermWriterConfig(0, mainComponents,
                                                                GlobalConfig.targetBlockSize)
    @volatile
    private var isOpen = true

    def getTermWriter(termID: Int): TermWriter = {
        if (termWriters.contains(termID)) {
            logger.trace("getTermWriter from cache termID = {}",termID)
            return termWriters.get(termID)
        }
        else {
            val conf = termWriterConfigTemplage.copy(termID)
            val newInstance = new TermWriter(conf)
            termWriters.putIfAbsent(termID, newInstance)
            logger.trace("getTermWriter new one termID = {}",termID)
            return termWriters.get(termID)
        }
    }

    //MONITORING SECTION
    private val _totalDocs = metrics.counter("totalDocs")
    private val _addedDocs = metrics.meter("addedDocs")
    private val _removedDocs = metrics.meter("removedDocs")
    private val _numScheduledCompactions = metrics.counter("numScheduledCompactions")
    private val _compactionsRunning = metrics.counter("compactionsRunning")
    private val _addDocumentTimer = metrics.timer("addDocumentTimer")
    private val _smallFlushTimer = metrics.timer("smallFlushTimer")
    private val _largeFlushTimer = metrics.timer("largeFlushTimer")

    private val compactionsRunning = new AtomicInteger(0)


    private lazy val docIDCounter = new AtomicLong(repository.getMaxDocID)
    @volatile
    private var smallFlush_lastDocID = repository.getMaxDocID

    private val modifyOrFlushLock = new ReentrantReadWriteLock()
    private val compactionsOrCloseLock = new ReentrantReadWriteLock()
    private val docSerializer = new DocumentSerializer (measureSerializer)

    def withSharedLock(func: ()=> Unit) = {
        logger.debug("-> withSharedLock")
        val sharedLock = modifyOrFlushLock.readLock()
        sharedLock.lock()
        try {
            func()
        } finally {
            sharedLock.unlock()
            logger.debug("withSharedLock ->")
        }
    }

    //========================= Merge Scheduling ===================================================
    private val scheduledCompactions = new ConcurrentLinkedDeque[ICompactionJob]

    def addCompactionJob(job: ICompactionJob): Unit = {
        logger.info("-> addCompactionJob ->")
        if(isOpen)  {
            scheduledCompactions.add(job)
            _numScheduledCompactions.inc()
        }
    }

    val fsRoot = Paths.get(".").toAbsolutePath.getRoot.toFile

    val timer = new Timer()
    timer.schedule(new TimerTask { override def run() = {
        if(isOpen && operatingSystemMXBean.getSystemCpuLoad <= GlobalConfig.merges_cpuLoadThreshold.get()) {
            while(isOpen && scheduledCompactions.nonEmpty &&
                compactionsRunning.get() < GlobalConfig.merges_maxConcurrent.get()) {
                //starting new compaction
                Future {
                    logger.info("timerTask starting new compaction")
                    compactionsOrCloseLock.readLock().lock()
                    compactionsRunning.incrementAndGet()
                    _compactionsRunning.inc()
                    try {
                        if(isOpen) {
                            val compactionToRun = scheduledCompactions.pollLast()
                            _numScheduledCompactions.dec()
                            //compute merges
                            val saveChangesHook = compactionToRun.computeChanges()
                            //save results of all the merges
                            val tw = getTermWriter(compactionToRun.termID)
                            saveChangesHook(withSharedLock, tw.withLock)
                        }
                    } finally {
                        compactionsOrCloseLock.readLock().unlock()
                        compactionsRunning.decrementAndGet()
                        _compactionsRunning.dec()
                    }
                }
            }
        }
    }}, GlobalConfig.merges_queueCheckInterval.get(),
        GlobalConfig.merges_queueCheckInterval.get())

    private val nSmallFlushesFromLastLarge = new AtomicInteger(0)

    timer.schedule(new TimerTask {
        override def run(): Unit = if(isOpen) {
            if(docIDCounter.get() > smallFlush_lastDocID) {
                logger.debug("timerTask for flush, nSmallFlushesFromLastLarge ={}",
                            nSmallFlushesFromLastLarge)
                smallFlush()
                val tmp = nSmallFlushesFromLastLarge.incrementAndGet()
                if (tmp == GlobalConfig.largeFlushInterval) {
                    largeFlush()
                    nSmallFlushesFromLastLarge.set(0)
                }
            }
        }
    }, GlobalConfig.smallFlushInterval,
        GlobalConfig.smallFlushInterval)



    val operatingSystemMXBean = management.ManagementFactory.getOperatingSystemMXBean
        .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    //==============================================================================================

    def getTotalDocs: Long = totalDocs.get()
    private val totalDocs = new AtomicLong(mainComponents.repository.getTotalDocsCount)

    def addDocument(document: AnalyzedDocument): Long = {
        logger.trace("addDocument Document = ", document)
        val sharedLock = modifyOrFlushLock.readLock()
        sharedLock.lock()
        var currentDocID = -1l
        try {
            currentDocID = docIDCounter.incrementAndGet()
            _addedDocs.mark()
            val context = _addDocumentTimer.time()
            document.terms.foreach(docTerm => {
                val docPosting = new DocPosting(currentDocID, docTerm.measure)
                getTermWriter(docTerm.termID).addDocument (docPosting)
            })
            val docSerialized = docSerializer.serialize(document)
            repository.addDocument(currentDocID, docSerialized)
            context.stop()
            totalDocs.incrementAndGet()
            _totalDocs.inc()
            return currentDocID
        } catch {
            case ex: Exception => logger.error("!!! addDocument", ex)
        } finally { sharedLock.unlock() }
        return currentDocID
    }

    def removeDocument(documentID: Long) = {
        logger.debug("removeDocument -- documentID = {}", documentID)
        val sharedLock = modifyOrFlushLock.readLock()
        sharedLock.lock()
        def logError: Nothing = {
            val ex = new scala.IllegalArgumentException("no such document exist")
            logger.error("!!! removeDocument", ex)
            throw ex
        }
        try {
            if (documentID > docIDCounter.get()) logError
            if (!metadataManager.isDeleted(documentID)) {
                val docBody = repository.getDocument(documentID)
                if (docBody.isEmpty) logError
                val document = docSerializer.deserialize(docBody.get)
                metadataManager.markDeleted(documentID)
                totalDocs.decrementAndGet()
                _removedDocs.mark()
                _totalDocs.dec()
                val operations = document.terms
                    .map(td => (td.termID, getTermWriter(td.termID)))
                    .toBuffer
                var operationTimeout = 1
                while (operations.nonEmpty) {
                    for (op <- operations) {
                        val ret = op._2.tryWithLock(
                            () => metadataManager.deleteFromTerm(op._1, documentID),
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
        } finally {
            if(exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def smallFlush() = {
        logger.info("-> smallFlush")
        val exclusiveLock = modifyOrFlushLock.writeLock()
        val wasAlreadyLocked = exclusiveLock.isHeldByCurrentThread
        if(!wasAlreadyLocked)
            exclusiveLock.lock()
        try {
            val context = _smallFlushTimer.time()
            termWriters.foreach(_._2.smallFlush())
            smallFlush_lastDocID = docIDCounter.get()
            context.stop()
        } finally {
            if(!wasAlreadyLocked && exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def largeFlush(): Unit = {
        logger.info("-> largeFlush")
        val exclusiveLock = modifyOrFlushLock.writeLock()
        val wasAlreadyLocked = exclusiveLock.isHeldByCurrentThread
        if(!wasAlreadyLocked)
            exclusiveLock.lock()
        try {
            val context = _largeFlushTimer.time()
            termWriters.foreach(_._2.largeFlush())
            val metadataToFlush = metadataManager.flush
            repository.flush(docIDCounter.get(),
                this.getTotalDocs,
                exclusiveLock,
                metadataToFlush)
            context.stop()
        } finally {
            if(!wasAlreadyLocked  && exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def close() = {
        isOpen = false
        compactionsOrCloseLock.writeLock().lock()
        try {
            timer.cancel()
            logger.info("close -- timer canceled")
            flush()
            logger.info("close -- data flushed")
            repository.close()
            logger.info("close -- index closed")
        } finally { compactionsOrCloseLock.writeLock().unlock() }
    }
}
