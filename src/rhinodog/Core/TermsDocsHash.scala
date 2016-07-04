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

import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.locks.ReentrantReadWriteLock

import Definitions._
import Configuration._
import org.slf4j.LoggerFactory
import rhinodog.Core.Utils._

import scala.collection._

import scala.collection.JavaConversions.mapAsScalaMap

case class TermKey(termID:Int, maxDocID: Long) extends Ordered[TermKey] {
    override def compare(that: TermKey): Int = {
        val termC = termID.compare(that.termID)
        if(termC != 0) return termC
        return maxDocID.compare(that.maxDocID)
    }
}

class TermsDocsHash
(config: TermsDocsHashConfig) {
    private val logger = LoggerFactory.getLogger(this.getClass)
    import config._

    val serializer = MetadataSerializer(mainComponents.measureSerializer)
    private var writeCache = new ConcurrentSkipListMap[TermKey, Measure]()

    private val flushTimer = mainComponents.metrics.timer("TermsDocsHash - flushTimer")

    /** use increasing timeouts for last terms of the document to finnish it's */
    def addDocument(docID: Long, docTerm: DocTerm): Boolean = {
        logger.trace("-> addDocuemnt -- termID = {} docID = {}", docTerm.termID, docID)
        writeCache.put(TermKey(docTerm.termID, docID), docTerm.measure)
        return true
    }

    def flush(exclusiveLock:  ReentrantReadWriteLock.WriteLock) = {
        val context = flushTimer.time()
        val start = System.currentTimeMillis()
        if (writeCache.size() > 0) {
            val data = writeCache
            writeCache = new ConcurrentSkipListMap[TermKey, Measure]()
            if(exclusiveLock != null && exclusiveLock.isHeldByCurrentThread)
                exclusiveLock.unlock()
            var currentTermID = data.headOption.get._1.termID
            var blocksManager = new BlocksWriter(mainComponents.measureSerializer,
                currentTermID,
                config.targetSize)

            data.foreach(entry => {
                if (currentTermID != entry._1.termID) {
                    //flush blocksManager content
                    val blocks = blocksManager.flushBlocks().values
                    for (block <- blocks) {
                        val key = block.key
                        val metaSerialized = serializer.serialize(block.meta)
                        mainComponents.repository.saveBlock(key, block.data, metaSerialized)
                        mainComponents.metadataManager.addMetadata(key, block.meta)
                    }
                    //reinit blocksManager
                    currentTermID = entry._1.termID
                    blocksManager = new BlocksWriter(mainComponents.measureSerializer,
                        currentTermID,
                        config.targetSize)
                }
                blocksManager.add(new DocPosting(entry._1.maxDocID, entry._2))
            })

        }
        val time = System.currentTimeMillis() - start
        logger.info("TermsDocsHash flush took {} ms",time)
        context.stop()
    }
}
