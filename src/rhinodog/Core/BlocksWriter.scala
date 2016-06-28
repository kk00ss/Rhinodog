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
import org.slf4j.LoggerFactory

import scala.collection.immutable.TreeMap
import scala.collection._

class BlocksWriter
(measureSerializer: MeasureSerializerBase,
 termID: Int = 0,
 blockTargetSize: Int = 4 * 1024 - 16) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    val docPostingsSerializer = new DocPostingsSerializer(measureSerializer)
    val segmentSize = DocPostingsSerializer.segmentSize

    var segmentBuffer = new mutable.ArrayBuffer[DocPosting](segmentSize)
    var prevDocID = 0l

    var currentBlockEstimatedSize = 0
    var currentBlocksSegments = List[Segment]()

    var blocks = new TreeMap[Long, BlockInfoRAM]()

    def add(docPosting: DocPosting): Unit = {
        logger.trace("add segmentBuffer.length = {}",segmentBuffer.length)
        if (docPosting.docID <= prevDocID) {
            val ex = new IllegalStateException("new docPosting should have " +
                "larger docID than the last one")
            logger.error("add",ex)
            throw ex
        }
        prevDocID = docPosting.docID
        if (segmentBuffer.length == segmentSize)
            flushSegment()
        segmentBuffer += docPosting
    }

    def bulkAdd(data: mutable.ArrayBuffer[DocPosting]) = {
        logger.trace("bulkAdd")
        //TODO: add docs until segment is filled and only then flush
        flushSegment()
        val sortedData = data.sortBy(_.docID)
        var i = 0
        val nSlices = sortedData.length / segmentSize
        while (i < nSlices + 1) {
            segmentBuffer = sortedData.slice(segmentSize*i,segmentSize)
            i += 1
            flushSegment()
        }
    }

    def flushSegment() = if (segmentBuffer.nonEmpty) {
        val serializedSegment: Segment = serializeSegment()
        var newSegmentSerializedSize = 4 + 8 + 2 + DocPostingsSerializer.sizeInBytes(serializedSegment.data)
        logger.trace("flushSegment newSegmentSerializedSize = {}", newSegmentSerializedSize)
        //measureSerializer.numberOfBytesRequired
        if (newSegmentSerializedSize + currentBlockEstimatedSize > blockTargetSize)
            flushBlock()
        currentBlockEstimatedSize += newSegmentSerializedSize
        logger.trace("flushSegment currentBlockEstimatedSize = {}", currentBlockEstimatedSize)
        currentBlocksSegments :+= serializedSegment
        segmentBuffer.clear()
    }

    def serializeSegment(): Segment = {
        logger.trace("serializeSegment")
        val data = docPostingsSerializer.encodeIntoComponents(segmentBuffer)
        var maxDocID = 0l
        var maxMeasure = measureSerializer.MinValue
        for (el <- segmentBuffer) {
            if (el.docID > maxDocID) maxDocID = el.docID
            if (el.measure.score > maxMeasure.score) maxMeasure = el.measure
        }
        Segment(maxDocID, maxMeasure, segmentBuffer.length, data)
    }

    def flushBlock() = if (currentBlocksSegments.nonEmpty) {
        var maxDocID = 0l
        var maxMeasureValue = measureSerializer.MinValue
        val buffer = ByteBuffer.allocate(currentBlockEstimatedSize)
        var totalDocsInBlock = 0
        currentBlocksSegments.foreach(segment => {
            totalDocsInBlock += segment.totalDocs
            if (segment.maxDocID > maxDocID) maxDocID = segment.maxDocID
            if (segment.maxMeasure.score > maxMeasureValue.score)
                maxMeasureValue = segment.maxMeasure
            buffer.putLong(segment.maxDocID)
            //measureSerializer.serialize(segment.maxMeasure, buffer)
            buffer.putFloat(segment.maxMeasure.score)
            val segmentLength = DocPostingsSerializer.sizeInBytes(segment.data).asInstanceOf[Short]
            buffer.putShort(segmentLength)
            DocPostingsSerializer.writeComponents(segment.data, buffer)
        })
        val meta = BlockMetadata(maxMeasureValue,
            currentBlockEstimatedSize,
            totalDocsInBlock)
        logger.trace("flushBlock meta = {}", meta)
        blocks += ((maxDocID, BlockInfoRAM(BlockKey(termID, maxDocID), meta, buffer.array())))
        currentBlockEstimatedSize = 0
        currentBlocksSegments = List()
    }

    def flushBlocks(): TreeMap[Long, BlockInfoRAM] = {
        logger.trace("flushBlocks nBlocks = {}", blocks.size)
        flushSegment()
        flushBlock()
        val oldBlocks = blocks
        blocks = new TreeMap[Long, BlockInfoRAM]()
        return oldBlocks
    }
}
