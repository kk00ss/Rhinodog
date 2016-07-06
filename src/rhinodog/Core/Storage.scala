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
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
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

//    val termsDocsHash
//    = new TermsDocsHash(TermsDocsHashConfig(mainComponents,GlobalConfig.index_targetBlockSize))
    val numCores = GlobalConfig.global_numCores
    val termsDocsHashes = new Array[TermsDocsHash](numCores)
    val termsDocsHashConfig = TermsDocsHashConfig(mainComponents,
                                                  GlobalConfig.storage_targetBlockSize)
    for(i <- 0 until numCores)
        termsDocsHashes(i) = new TermsDocsHash(termsDocsHashConfig)

    @volatile
    private var isOpen = true

    //MONITORING SECTION
    private val _totalDocs = metrics.counter("storage - totalDocs")
    private val _removedDocs = metrics.meter("storage - removedDocs")
    private val _numScheduledCompactions = metrics.counter("storage - numScheduledCompactions")
    private val _compactionsRunning = metrics.counter("storage - compactionsRunning")
    private val _addDocumentTimer = metrics.timer("storage - addDocumentTimer")
    private val _flushTimer = metrics.timer("storage - flushTimer")

    private val compactionsRunning = new AtomicInteger(0)


    private lazy val docIDCounter = new AtomicLong(repository.getMaxDocID)
    @volatile
    private var lastDocIDFlushed = repository.getMaxDocID

    private val modifyOrFlushLock = new ReentrantReadWriteLock()
    private val compactionsOrCloseLock = new ReentrantReadWriteLock()
    private val docSerializer = new DocumentSerializer (measureSerializer)

    //========================= Merge Scheduling =============================================
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
                compactionsRunning.get() < GlobalConfig.global_numCores) {
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
                            val writeLock = modifyOrFlushLock.writeLock()
                            writeLock.lock()
                            try {
                                saveChangesHook()
                            } finally {
                                writeLock.unlock()
                            }
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

    if(GlobalConfig.storage_autoFlush) {
        timer.schedule(new TimerTask {
            override def run(): Unit = if (isOpen) {
                if (docIDCounter.get() > lastDocIDFlushed) {
                    logger.debug("timerTask for flush")
                        flush()
                 }
            }
        }, GlobalConfig.storage_flushInterval,
            GlobalConfig.storage_flushInterval)
    }


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
            val context = _addDocumentTimer.time()
            document.terms.foreach(docTerm => {
                val hashIndex = docTerm.termID % numCores
                termsDocsHashes(hashIndex).addDocument (currentDocID, docTerm)
            })
            val docSerialized = docSerializer.serialize(document)
            repository.addDocument(currentDocID, docSerialized)
            context.stop()
            totalDocs.incrementAndGet()
            _totalDocs.inc()
            return currentDocID
        } catch {
            case ex: Exception => {
                logger.error("!!! addDocument", ex)
                throw ex
            }
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
                document.terms.foreach(x => metadataManager.deleteFromTerm(x.termID, documentID))
            }
        } finally {
            sharedLock.unlock()
        }
    }

    val flushInProgressLock = new ReentrantLock()

    def flush(): Unit = {
        logger.info("-> largeFlush")
        val exclusiveLock = modifyOrFlushLock.writeLock()
        val wasAlreadyLocked = exclusiveLock.isHeldByCurrentThread
        if(!wasAlreadyLocked)
            exclusiveLock.lock()
        try {
            val context = _flushTimer.time()
            termsDocsHashes.foreach(_.rotateWriteLog())
            exclusiveLock.unlock()
            termsDocsHashes.par.foreach(_.flush())
            flushInProgressLock.lock()
            val metadataToFlush = metadataManager.flush
            repository.flush(docIDCounter.get(),
                this.getTotalDocs,
                flushInProgressLock,
                metadataToFlush)
            context.stop()
        } catch {
            case ex: Exception => logger.error("!!! flush failed ",ex)
        } finally {
            if(!wasAlreadyLocked  && exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
            if(flushInProgressLock.isHeldByCurrentThread)
                flushInProgressLock.unlock()
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
        } finally { compactionsOrCloseLock.writeLock().unlock() }
    }
}
