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

import java.nio.ByteBuffer

import Definitions._
import Utils.DocPostingsSerializer

import scala.collection.immutable.TreeMap
import scala.collection._

class BlocksWriter
(measureSerializer: MeasureSerializerBase,
 termID: Int = 0,
 blockTargetSize: Int = 4 * 1024 - 16) {

    val docPostingsSerializer = new DocPostingsSerializer(measureSerializer)
    val segmentSize = DocPostingsSerializer.segmentSize

    var currentPostingPosition = 0
    var segmentBuffer = new Array[DocPosting](segmentSize)
    var prevDocID = 0l

    var currentBlockEstimatedSize = 0
    var currentBlocksSegments = List[Segment]()

    var blocks = new mutable.ArrayBuffer[BlockInfoRAM]()

    def add(docPosting: DocPosting): Unit = {
        if (docPosting.docID <= prevDocID)
            throw new IllegalStateException("new docPosting should have larger docID than the last one")
        prevDocID = docPosting.docID
        if (currentPostingPosition == segmentSize)
            flushSegment()
        segmentBuffer(currentPostingPosition) = docPosting
        currentPostingPosition += 1

    }

    def bulkAdd(data: Seq[DocPosting]) = {
        flushSegment()
        var sortedData = data.sortBy(_.docID).toArray
        currentPostingPosition = segmentSize
        while (sortedData.nonEmpty) {
            segmentBuffer = sortedData.take(segmentSize)
            sortedData = sortedData.drop(segmentSize)
            flushSegment()
        }
        currentPostingPosition = 0
    }

    def flushSegment() = if(currentPostingPosition > 0) {
        val serializedSegment: Segment = serializeSegment()
        var newSegmentSerializedSize = measureSerializer.numberOfBytesRequired + 8 + 2 +
            DocPostingsSerializer.sizeInBytes(serializedSegment.data)
        if (newSegmentSerializedSize + currentBlockEstimatedSize > blockTargetSize)
            flushBlock()
        currentBlockEstimatedSize += newSegmentSerializedSize
        currentBlocksSegments :+= serializedSegment
        segmentBuffer = new Array[DocPosting](segmentSize)
        currentPostingPosition = 0
    }

    def serializeSegment(): Segment = {
        val data = docPostingsSerializer.encodeIntoComponents(segmentBuffer.take(currentPostingPosition))
        var maxDocID = 0l
        var maxMeasure = measureSerializer.MinValue
        for (i <- 0 until currentPostingPosition) {
            val el = segmentBuffer(i)
            if (el.docID > maxDocID) maxDocID = el.docID
            if (el.measure.score > maxMeasure.score) maxMeasure = el.measure
        }
        Segment(maxDocID, maxMeasure, data)
    }

    def flushBlock() = if(currentBlocksSegments.nonEmpty) {
        var maxDocID = 0l
        var maxMeasureValue = measureSerializer.MinValue
        val buffer = ByteBuffer.allocate(currentBlockEstimatedSize)
        currentBlocksSegments.foreach(segment => {
            if (segment.maxDocID > maxDocID) maxDocID = segment.maxDocID
            if (segment.maxMeasure.score > maxMeasureValue.score)
                maxMeasureValue = segment.maxMeasure
            buffer.putLong(segment.maxDocID)
            measureSerializer.serialize(segment.maxMeasure, buffer)
            val segmentLength = DocPostingsSerializer.sizeInBytes(segment.data).asInstanceOf[Short]
            buffer.putShort(segmentLength)
            DocPostingsSerializer.writeComponents(segment.data, buffer)
        })
        val meta = BlockMetadata(maxMeasureValue,
                                 currentBlockEstimatedSize,
                                 segmentSize * currentBlocksSegments.length)
        blocks :+= BlockInfoRAM(BlockKey(termID,maxDocID), meta, buffer.array())
        currentBlockEstimatedSize = 0
        currentBlocksSegments = List()
    }

    def flushBlocks(): mutable.ArrayBuffer[BlockInfoRAM] = {
        flushSegment()
        flushBlock()
        val oldBlocks = blocks
        blocks = new mutable.ArrayBuffer[BlockInfoRAM]()
        return oldBlocks
    }
}
