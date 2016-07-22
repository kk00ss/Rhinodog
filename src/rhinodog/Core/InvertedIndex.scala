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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import Definitions._
import Configuration._
import com.codahale.metrics.{Slf4jReporter, JmxReporter, ConsoleReporter, MetricRegistry}
import org.slf4j.{Marker, LoggerFactory}
import rhinodog.Analysis.LocalLexicon
import rhinodog.Core.Definitions.BaseTraits.IAnalyzer
import rhinodog.Core.Utils.DocumentSerializer

//TODO: Config and measureSerializer type name could be read from storage if index is already initialized, or at least check if they are the same
class InvertedIndex
(val measureSerializer: MeasureSerializerBase,
 val analyzer: IAnalyzer,
 val storageMode: storageModeEnum.storageMode = storageModeEnum.READ_WRITE) {
    private val logger = LoggerFactory.getLogger(this.getClass)
    // этот лок нужен только для того что бы флаш случился только после того
    // как определённый неявный набор документов
    // был проанализирован - только для предсказуемого поведения,
    // ( все документы для которых был вызван analyzeDocument до flush,
    // должны быть проанализированны в том же flush / transaction)
    private val sharedExclusiveLock = new ReentrantReadWriteLock(true)
    private var storage: Storage = null
    var _mainComponents: MainComponents = null
    private var queryEnginge: QueryEngine = null

    val metrics = new MetricRegistry()

    def getQueryEngine() = queryEnginge

    @volatile
    private var isClosed = false

    init()

    private def init() = {
        logger.info("-> init")
        val watermark = "measureSerializer name: " + measureSerializer.getClass.getCanonicalName +
            " analyzer name: " + analyzer.getClass.getCanonicalName
        val repository = new Repository(storageMode, metrics, watermark)
        try {
            val metadataManager = new MetadataManager(measureSerializer)

            val mainComponents = MainComponents(measureSerializer,
                repository,
                metadataManager,
                metrics)
            val compactionManager = new CompactionManager(mainComponents)
            this.storage = new Storage(mainComponents)

            compactionManager.addCompactionJob = this.storage.addCompactionJob
            metadataManager.mergeDetector = compactionManager
            repository.restoreMetadata(metadataManager.restoreMetadataHook)
            this.queryEnginge = new QueryEngine(mainComponents, storage)
            this._mainComponents = mainComponents

            logger.info("info -- init compete, configuring reporting")
            //Reporting metricx
            val reportingInterval = GlobalConfig.metrics_reportingInterval
            val rateTimeUnit = if (GlobalConfig.metrics_rates) TimeUnit.SECONDS
            else TimeUnit.MINUTES
            if (GlobalConfig.metrics_slf4j) {
                val slf4jReporter = Slf4jReporter.forRegistry(metrics)
                    .outputTo(LoggerFactory.getLogger("com.example.metrics"))
                    .convertRatesTo(rateTimeUnit)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build()
                slf4jReporter.start(reportingInterval, TimeUnit.SECONDS)
            }
            if (GlobalConfig.metrics_jmx) {
                val jmxReporter = JmxReporter.forRegistry(metrics)
                    .convertRatesTo(rateTimeUnit)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build()
                jmxReporter.start()
            }
        } catch {
            case e: Exception => {
                logger.error("!!! Initialization failed ", e)
                this.close()
                throw e
            }
        }
        logger.info("info ->")
    }

    //diagnostics only
    val nAnalysisThreadsActive = new AtomicInteger(0)

    val analyzeDocumentTimer = metrics.timer("AnalyzeDocumentTimer")

    /*returns ID of a new document, or -1 if failed to create one*/
    def addDocument(document: Document): Long = {
        logger.trace("addDocument Document = {}", document)
        if (isClosed) {
            val ex = new IllegalStateException("Index is closed")
            logger.debug("!!! removeDocument", ex)
            throw ex
        }
        var success = false
        val sharedLock = sharedExclusiveLock.readLock()
        sharedLock.lock()
        nAnalysisThreadsActive.incrementAndGet()
        var docID = -1l
        try {
            //TODO: Implement pooling ?
            val context = analyzeDocumentTimer.time()
            val lexicon = new LocalLexicon(_mainComponents.repository)
            val analyzedDocument = analyzer.analyze(document, lexicon)
            context.stop()
            docID = storage.addDocument(analyzedDocument)
            success = true
        } catch {
            case ex: Exception => {
                logger.error("!!! addDocument", ex)
                this.close()
                throw ex
            }
        } finally {
            nAnalysisThreadsActive.decrementAndGet()
            sharedLock.unlock()
        }
        return docID
    }

    def removeDocument(docID: Long): Unit = {
        logger.debug("removeDocument docID = {}", docID)
        if (isClosed) {
            val ex = new IllegalStateException("Index is closed")
            logger.error("!!! removeDocument", ex)
            throw ex
        }
        var success = false
        val sharedLock = sharedExclusiveLock.readLock()
        sharedLock.lock()
        try {
            storage.removeDocument(docID)
            success = true
        } catch {
            case e: Exception => {
                logger.error("!!! removeDocument", e)
                this.close()
                throw e
            }
        } finally {
            sharedLock.unlock()
        }
    }

    val docSerializer = new DocumentSerializer(this.measureSerializer)

    def getDocument(ID: Long): Option[AnalyzedDocument] = {
        val serializedDoc = this._mainComponents.repository.getDocument(ID)
        serializedDoc.map(x => docSerializer.deserialize(x))
    }

    def flush() = {
        logger.info("-> flush")
        var success = false
        val exclusiveLock = sharedExclusiveLock.writeLock()
        try {
            storage.flush()
            success = true
        } catch {
            case e: Exception => {
                logger.error("!!! flush", e)
                this.close()
                throw e
            }
        } finally {
            if(exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def close() = {
        logger.info("-> close")
        isClosed = true
        val exclusiveLock = sharedExclusiveLock.writeLock()
        exclusiveLock.lock()
        try {
            storage.close()
            _mainComponents.repository.close()
            logger.info("close -- index closed")
        } catch {
            case e: Exception => logger.debug("!!! close", e)
        } finally {
            exclusiveLock.unlock()
        }
        logger.info("closed gracefully")
    }
}
