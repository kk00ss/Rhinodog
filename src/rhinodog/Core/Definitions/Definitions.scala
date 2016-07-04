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

import rhinodog.Core.Definitions.BaseTraits.ISnapshotReader

import scala.collection._
import scala.collection.immutable.TreeMap

package object Definitions {

    case class IndexProperties()

    case class DocPosting
    (docID: Long, measure: Measure) extends Ordered[DocPosting] {
        if (docID == 0) throw new IllegalArgumentException("DocID should start from 1, not zero")
        def score = measure.score
        def compare(that: DocPosting) = this.docID.compareTo(that.docID)
    }

    case class DocTerm
    (termID: Int,
     measure: Measure)

    case class Document
    (text: String)

    case class AnalyzedDocument
    (terms: Seq[DocTerm])

    type SegmentSerialized = Array[Array[Int]]

    case class Segment
    (maxDocID: Long,
     maxMeasure: Measure,
     totalDocs: Int,
     data: SegmentSerialized)

    type BlockDataSerialized = Array[Byte]
    type BlockMetadataSerialized = Array[Byte]
    type WithLockFunc = (Function0[Unit]) => Unit

    //TODO: maybe add ordering to avoid storing separate Long keys ???
    trait BlockInfoBase {
        def maxDocID: Long
        def maxMeasure: Measure
        def data: Array[Byte]
    }

    case class BlockInfoRAM
    (key: BlockKey,
     meta: BlockMetadata,
     rowData: Array[Byte])
        extends BlockInfoBase with Ordered[BlockInfoRAM] {
        def maxDocID = key.maxDocID
        def maxMeasure = meta.maxMeasureValue
        def data: Array[Byte] = rowData
        def compare(that: BlockInfoRAM): Int = this.maxDocID.compareTo(that.maxDocID)
    }

    case class BlockInfo
    (key: BlockKey,
     meta: BlockMetadata,
     snapshotReader: ISnapshotReader)
        extends BlockInfoBase with Ordered[BlockInfo] {
        def maxDocID = key.maxDocID
        def maxMeasure = meta.maxMeasureValue
        private var dataCache: Array[Byte] = null
        def data: Array[Byte] = {
            if(dataCache == null)
                dataCache = snapshotReader.getBlock(key)
            return dataCache
        }
        def compare(that: BlockInfo): Int = this.maxDocID.compareTo(that.maxDocID)
    }


    case class BlockMetadata
    (maxMeasureValue: Measure,
     encodedSize: Int,
     totalNumber: Int,
     var changeByte: Byte = 0,
     var compactionLevel: Byte = 0,
     //stored separately
     var numberOfDeleted: Int = 0) {
        def fillFactor = if(numberOfDeleted > 0)
            (totalNumber-numberOfDeleted)/numberOfDeleted
        else 1
    }

    type DocumentSerialized = Array[Byte]

    //key should be assigned as docID / some step
    case class BitSetSegmentSerialized(key: Long, data: Array[Byte])

    //changes to this DS should be controlled by TermWriter.addOrFlushLock
    case class TermMetadata
    (var blocks: TreeMap[BlockKey, BlockMetadata]
     = new TreeMap[BlockKey, BlockMetadata](),
     var numberOfDocs: Long = 0)

    case class MetadataToFlush
    (bitSetSegments: Seq[BitSetSegmentSerialized],
     deletionInfo: java.util.concurrent.ConcurrentHashMap[BlockKey, Int])
}
