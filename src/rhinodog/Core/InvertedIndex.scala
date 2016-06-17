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

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import Definitions._
import Configuration._
import rhinodog.Analysis.LocalLexicon
import rhinodog.Core.Definitions.BaseTraits.IAnalyzer

//TODO: Config and measureSerializer type name could be read from storage if index is already initialized, or at least check if they are the same
class InvertedIndex
(val measureSerializer: MeasureSerializerBase,
 val analyzer: IAnalyzer,
 val storageMode: storageModeEnum.storageMode = storageModeEnum.READ_WRITE) {
    // этот лок нужен только для того что бы флаш случился только после того
    // как определённый неявный набор документов
    // был проанализирован - только для предсказуемого поведения,
    // ( все документы для которых был вызван analyzeDocument до flush,
    // должны быть проанализированны в том же flush / transaction)
    private val sharedExclusiveLock = new ReentrantReadWriteLock(true)
    private var storage: Storage = null
    private var _mainComponents: MainComponents = null
    private var queryEnginge: QueryEngine = null

    def getQueryEngine() = queryEnginge

    @volatile
    private var isClosed = false

    init()

    private def init() = {
        val watermark = "measureSerializer name: " + measureSerializer.getClass.getCanonicalName +
            " analyzer name: " + analyzer.getClass.getCanonicalName
        val repository = new Repository(storageMode, watermark)
        val metadataManager = new MetadataManager(measureSerializer)

        val mainComponents = MainComponents(measureSerializer, repository, metadataManager)
        val compactionManager = new CompactionManager(mainComponents)
        this.storage = new Storage(mainComponents)

        compactionManager.addCompactionJob = this.storage.addCompactionJob
        metadataManager.mergeDetector = compactionManager
        repository.restoreMetadata(metadataManager.restoreMetadataHook)
        this.queryEnginge = new QueryEngine(mainComponents, storage)
        this._mainComponents = mainComponents
    }

    //diagnostics only
    val nAnalysisThreadsActive = new AtomicInteger(0)

    /*returns ID of a new document, or -1 if failed to create one*/
    def addDocument(document: Document): Long = {
        if (isClosed)
            throw new IllegalStateException("Index is closed")
        val sharedLock = sharedExclusiveLock.readLock()
        sharedLock.lock()
        nAnalysisThreadsActive.incrementAndGet()
        var docID = -1l
        try {
            //TODO: Implement pooling ?
            val lexicon = new LocalLexicon(_mainComponents.repository)
            val analyzedDocument = analyzer.analyze(document, lexicon)
            docID = storage.addDocument(analyzedDocument)
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            nAnalysisThreadsActive.decrementAndGet()
            sharedLock.unlock()
        }
        return docID
    }

    def removeDocument(docID: Long): Unit = {
        if (isClosed)
            throw new IllegalStateException("Index is closed")
        val sharedLock = sharedExclusiveLock.readLock()
        sharedLock.lock()
        try {
            storage.removeDocument(docID)
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            sharedLock.unlock()
        }
    }

    def flush() = {
        val exclusiveLock = sharedExclusiveLock.writeLock()
        try {
            storage.flush()
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            if(exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
        }
    }

    def close() = {
        isClosed = true
        val exclusiveLock = sharedExclusiveLock.writeLock()
        exclusiveLock.lock()
        try {
            storage.flush()
            storage.close()
        } catch {
            case e: Exception => {
                e.printStackTrace()
            }
        } finally {
            exclusiveLock.unlock()
        }
    }
}
