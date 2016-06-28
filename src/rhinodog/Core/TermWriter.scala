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
import java.util.concurrent.locks.ReentrantLock

import Definitions._
import Configuration._
import Iterators._
import org.slf4j.LoggerFactory
import rhinodog.Core.Utils._

import scala.collection._

object TermWriter {
    private val logger = LoggerFactory.getLogger(this.getClass)
}

class TermWriter
(config: TermWriterConfig) {

    import config._

    val serializer = MetadataSerializer(mainComponents.measureSerializer)

    val blocksManager = new BlocksWriter(mainComponents.measureSerializer,
        config.termID,
        config.targetSize)

    //for small flush interval during which analysis threads may add documents out of order
    //TODO: compare with thread-safe data structure
    private var writeCache = SortedSet[DocPosting]()
    private var minDocID = Long.MaxValue
    private var maxDocID = 0l

    //probably a bit misplaced lock, it controls access to term metadata in MetadataManager
    //for new trees and for compactions, that is why we need withLock functions
    private val addOrFlushLock = new ReentrantLock()

    private val _smallFlushTimer = mainComponents.metrics.timer("TermWriter SmallFlush")
    private val _largeFlushTimer = mainComponents.metrics.timer("TermWriter LargeFlush")

    def getIterator = {
        TermWriter.logger.trace("-> getIterator ->")
        new RowSegmentIterator(writeCache.toSeq)
    }

    def withLock(func: Function0[Unit]) = {
        TermWriter.logger.trace("-> withLock")
        addOrFlushLock.lock()
        try {
            func()
        } finally {
            addOrFlushLock.unlock()
            TermWriter.logger.trace("withLock ->")
        }
    }

    def tryWithLock(func: Function0[Unit], timeout: Long = 0): Boolean = {
        TermWriter.logger.trace("-> withLock")
        val attemptResult = addOrFlushLock.tryLock(timeout, TimeUnit.MILLISECONDS)
        if (!attemptResult) return false
        else {
            try {
                func()
            } finally {
                addOrFlushLock.unlock()
                TermWriter.logger.trace("withLock ->")
            }
        }
        return true
    }

    /** use increasing timeouts for last terms of the document to finnish it's */
    def addDocument(doc: DocPosting): Boolean = {
        TermWriter.logger.trace("-> addDocuemnt -- termID = {} docPosting = {}",
                                config.termID, doc)
        addOrFlushLock.lock()
        try {
            //Analysis threads could finnish processing in different order than they started
            if (doc.docID < minDocID) minDocID = doc.docID
            if (doc.docID > maxDocID) maxDocID = doc.docID
            writeCache += doc
            return true
        } finally {
            addOrFlushLock.unlock()
            TermWriter.logger.trace("addDocument ->")
        }
    }

    def smallFlush() = {
        addOrFlushLock.lock()
        try {
            if (writeCache.nonEmpty) {
                val context = _smallFlushTimer.time()
                writeCache.foreach(blocksManager.add)
                writeCache = SortedSet[DocPosting]()
                context.stop()
            }
        } finally {
            addOrFlushLock.unlock()
        }
    }

    def largeFlush() = {
        addOrFlushLock.lock()
        try {
            val context = _smallFlushTimer.time()
            val blocks = blocksManager.flushBlocks().values
            for (block <- blocks) {
                val key = block.key
                val metaSerialized = serializer.serialize(block.meta)
                mainComponents.repository.saveBlock(key, block.data, metaSerialized)
                mainComponents.metadataManager.addMetadata(key, block.meta)
            }
            context.stop()
        } finally {
            addOrFlushLock.unlock()
        }
    }
}
