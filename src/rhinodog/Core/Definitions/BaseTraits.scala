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
import rhinodog.Core.Definitions.Caching.BlockCache
import rhinodog.Core.PartialFlushWAL

import scala.collection._

import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock

object BaseTraits {

    trait Analyzer {
        val analyzerName: String
        def analyze(text: String,
                    docMetadata: mutable.Map[String,String],
                    analyzerConfig: mutable.Map[String,String]): AnalyzedDocument
    }

    trait TermIteratorBase {
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
    }

    trait CompactionJobInterface {
        val termID: Int
        //returned function should only be run with TermWriter.tryWithLock
        def computeChanges(): SaveChangesHook
    }

    trait CompactionManagerInterface {
        def blockAddedEvent(termID: Int, key: BlockKey, meta: BlockMetadata): Unit
        def docDeletedEvent(key: BlockKey, meta: BlockMetadata): Unit
        //might be used to compute adaptive merge factor
        def updateFreeSpaceRatio(newValue: Float)
    }

    case class RestoreMetadataHook
    (processMetadata: (BlockKey, ByteBuffer) => Unit,
     processNumDeleted: (BlockKey, Int) => Unit,
     processBitSetSegment: BitSetSegmentSerialized => Unit)

    trait MetadataManagerBase {
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
        def isDeleted(docID: Long): Boolean
        def markDeleted(docID: Long): Unit
        //should only be run with TermWriter.addOrFlushLock
        def deleteFromTerm(termID: Int, measure: Measure, docID: Long): Boolean
        def getTermMetadata(termID: Int): Option[TermMetadata]
        //should only be run global lock in storage
        def flush: MetadataToFlush
    }

    trait SnapshotReaderInterface {
        def getBlock(blockKey: BlockKey): BlockDataSerialized
        def seekBlock(termID: Int, docID: Long): (Long, BlockDataSerialized)
        def close(): Unit
    }

    trait RepositoryBase {
        // lambda expects termID, metadataSerialized
        def restoreMetadata(restoreMetadataHook: RestoreMetadataHook)
        def getMaxDocID: Long

        def getSnapshotReader: SnapshotReaderInterface

        def writePartialFlushWAL(data: PartialFlushWAL): Array[Byte]
        def deletePartialFlushWAL(key: Array[Byte]): Unit

        def saveBlock(key: BlockKey,
                      block: BlockDataSerialized,
                      metadata: BlockMetadataSerialized): Unit
        //required for merging - will be able to substitute saveBlock everywhere
        def replaceBlocks(deletedBlocks: Seq[BlockKey], blocks: Seq[BlockCache]): Unit

        def flush(lastDocIDAssigned: Long,
                  storageLock: WriteLock,
                  metadataFlush: MetadataToFlush): Unit
        def close(): Unit

        def addDocument(docID: Long, doc: DocumentSerialized): Unit
        def getDocument(docID: Long): Option[DocumentSerialized]
        def removeDocument(docID: Long): Unit
        //adds term to lexicon if it wasn't present
        def getTermID(term: String): Int
        def getTerm(id: Int): String
    }
}
