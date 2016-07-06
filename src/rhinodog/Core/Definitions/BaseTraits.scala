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
package rhinodog.Core.Definitions


import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import rhinodog.Analysis.LocalLexicon
import rhinodog.Core.Definitions.Caching.BlockCache

import scala.collection._

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock

object BaseTraits {

    trait IFacetCollector {

    }

    trait IAnalyzer {
        val analyzerName: String
        def analyze(doc: Document, lexicon: LocalLexicon): AnalyzedDocument
    }

    trait ITermIterator {
        def currentDocID: Long
        def currentScore: Float

        def blockMaxDocID: Long
        def blockMaxScore: Float
        def segmentMaxDocID: Long
        def segmentMaxScore: Float

        def nextBlock()

        def nextSegmentMeta()
        def initSegmentIterator()

        def hasNextBlock: Boolean
        def hasNextSegment: Boolean
        def hasNext: Boolean

        def next(): Long
        def advanceToScore(targetScore: Float): Long
        def advance(targetDocID: Long): Long

        def close()
    }

    trait ICompactionJob {
        val termID: Int
        //returned function should only be run with TermWriter.tryWithLock
        def computeChanges(): Function0[Unit]
    }

    trait ICompactionManager {
        def blockAddedEvent(termID: Int, key: BlockKey, meta: BlockMetadata): Unit
        def docDeletedEvent(key: BlockKey, meta: BlockMetadata): Unit
    }

    case class RestoreMetadataHook
    (processMetadata: (BlockKey, ByteBuffer) => Unit,
     processNumDeleted: (BlockKey, Int) => Unit,
     processBitSetSegment: BitSetSegmentSerialized => Unit)

    trait IMetadataManager {
        val restoreMetadataHook: RestoreMetadataHook
        //should only be run with TermWriter.addOrFlushLock
        def addMetadata(blockKey: BlockKey,
                        meta: BlockMetadata): TermMetadata
        //should only be run with TermWriter.addOrFlushLock
        def replaceMetadata(termID: Int,
                            deletedTrees: Seq[BlockKey],
                            added: Seq[(BlockKey, BlockMetadata)])
        //def iterator(termID: Int): Option[MetadataIterator]
        def getNumberOfDocs(termID: Int): Long
        def getMostFrequentTerms(num: Int): Array[Int]

        def isDeleted(docID: Long): Boolean
        def markDeleted(docID: Long): Unit
        //should only be run with TermWriter.addOrFlushLock
        def deleteFromTerm(termID: Int, docID: Long): Boolean
        def getTermMetadata(termID: Int): Option[TermMetadata]
        //should only be run global lock in storage
        def flush: MetadataToFlush
    }

    trait ISnapshotReader {
        def getBlock(blockKey: BlockKey): BlockDataSerialized
        def seekBlock(termID: Int, docID: Long): (Long, BlockDataSerialized)
        def close(): Unit
    }

    trait IRepository extends {
        //Documents related
        def addDocument(docID: Long, doc: DocumentSerialized): Unit
        def getDocument(docID: Long): Option[DocumentSerialized]
        def removeDocument(docID: Long): Unit

        //adds term to lexicon if it wasn't present
        def getTermID(term: String): Int
        def getTerm(id: Int): String

        // lambda expects termID, metadataSerialized
        def restoreMetadata(restoreMetadataHook: RestoreMetadataHook)
        def getMaxDocID: Long
        def getTotalDocsCount: Long

        def getSnapshotReader: ISnapshotReader
        def saveBlock(key: BlockKey,
                      block: BlockDataSerialized,
                      metadata: BlockMetadataSerialized): Unit
        //required for merging
        def replaceBlocks(deletedBlocks: Seq[BlockKey], blocks: Seq[BlockCache]): Unit

        def flush(lastDocIDAssigned: Long,
                  newTotalDocsCount: Long,
                  storageLock: ReentrantLock,
                  metadataFlush: MetadataToFlush): Unit
        def close(): Unit
    }
}
