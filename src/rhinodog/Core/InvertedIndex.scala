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

//TODO: Implement InvertedIndex with higher abstraction, which will be able to have
//TODO: multiply storage engines for indexing performance
class InvertedIndex
(val config: GlobalConfig,
 val measureSerializer: MeasureSerializerBase) {
    // этот лок нужен только для того что бы флаш случился только после того
    // как определённый неявный набор документов
    // был проанализирован - только для предсказуемого поведения,
    // ( все документы для которых был вызван analyzeDocument до flush,
    // должны быть проанализированны в том же flush / transaction)
    private val sharedExclusiveLock = new ReentrantReadWriteLock(true)
    private var storage: Storage = null

    @volatile
    private var isClosed = false

    init()
    private def init() = {
        val repository = new Repository(config)
        val metadataManager = new MetadataManager(config, measureSerializer)

        val mainComponents = MainComponents(measureSerializer,repository,metadataManager) //,null)
//        val blockReader = new BlockReader(mainComponents)
//        mainComponents.blockReader = blockReader
        val compactionManager = new CompactionManager(config, mainComponents)
        val storageConfig = StorageConfig(config, mainComponents, compactionManager.updateFreeSpaceRatio)
        this.storage = new Storage(storageConfig)

        compactionManager.addCompactionJob = this.storage.addCompactionJob
        metadataManager.mergeDetector = compactionManager
        repository.restoreMetadata(metadataManager.restoreMetadataHook)
    }

    //diagnostics only
    val nAnalysisThreadsActive = new AtomicInteger(0)

    /*returns ID of a new document, or -1 if failed to create one*/
    def addDocument(text: String): Long = {
        if(isClosed)
            throw new IllegalStateException("Index is closed")
        val sharedLock = sharedExclusiveLock.readLock()
        sharedLock.lock()
        nAnalysisThreadsActive.incrementAndGet()
        var docID = -1l
        try {
            //do stuff
            //analyze
            val docInfo: AnalyzedDocument = null
            docID = storage.addDocument(docInfo)
        } catch {
            case e: Exception => { e.printStackTrace() }
        } finally {
            nAnalysisThreadsActive.decrementAndGet()
            sharedLock.unlock()
        }
        return docID
    }

    def removeDocument(docID: Long): Unit = {
        if(isClosed)
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
