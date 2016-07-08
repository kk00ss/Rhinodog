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
package rhinodog.Core.Iterators

import java.nio.ByteBuffer

import org.slf4j.LoggerFactory
import rhinodog.Core.Definitions._
import BaseTraits.ITermIterator
import rhinodog.Core.Utils.DocPostingsSerializer

//TODO: add softNot and strongNot which would lower estimate for documents

object BlocksIterator {
    def computeIDF(totalDocs: Long, termFrequency: Long): Float = {
        var result =
            math.log(1+(totalDocs - termFrequency + 0.5D)
                / (termFrequency + 0.5D)).asInstanceOf[Float]
        if(result < 0) result = 0
        result
    }
}

class BlocksIterator
(metaIterator: MetadataIteratorBase,
 measureSerializer: MeasureSerializerBase,
 termFrequency: Long,
 totalDocs: Long,
 useIDF: Boolean = false) extends ITermIterator {
    private val logger = LoggerFactory.getLogger(this.getClass)

    if(termFrequency == 0) {
        val ex =  new IllegalArgumentException("termFrequency cannot be 0, " +
            "term should be excluded from query")
        logger.error("BlocksIterator constructor", ex)
        throw ex
    }

    lazy val IDF: Float = if(useIDF) computeIDF else 1f

    private def computeIDF: Float = {
        val tmp = BlocksIterator.computeIDF(totalDocs, termFrequency)
        logger.trace("computeIDF termFrequency = {}, IDF = {}", termFrequency, tmp)
        return tmp
    }

    private var block:  (Long, BlockInfoBase) = metaIterator.currentElement
    private var blockBuffer: ByteBuffer = ByteBuffer.wrap(block._2.data)
    private var _blockMaxDocID: Long = block._1
    private var _blockMaxScore: Float = block._2.maxMeasure.score * IDF

    val numComponents = measureSerializer.numberOfComponentsRequired + 1
    var maxDocIDsOffset = 0 //uninitialized value

    val segmentReadBuffer = new SegmentSerialized(numComponents)
    val encodedLengths = new Array[Int](numComponents)
    val segmentDecodedBuffer = new SegmentSerialized(numComponents)

    //large longs can take up to 3 Int's to encode (0 for a flag)
    segmentReadBuffer(0) = new Array[Int](DocPostingsSerializer.segmentSize * 3)
    segmentDecodedBuffer(0) = new Array[Int](DocPostingsSerializer.segmentSize * 3)
    for(i <- 1 to measureSerializer.numberOfComponentsRequired) {
        //sometimes compression works the opposite way - so buffers should be a bit larger
        segmentReadBuffer(i) = new Array[Int](DocPostingsSerializer.segmentSize+16)
        segmentDecodedBuffer(i) = new Array[Int](DocPostingsSerializer.segmentSize)
    }

    private var _segmentMaxDocID: Long = 0
    private var _segmentMaxScore: Float = 0
    private var segmentSkip: Short = 0
    private val segmentIterator = new SerializedSegmentIterator(segmentDecodedBuffer,
        measureSerializer,
        maxDocIDsOffset)

    def currentDocID: Long = segmentIterator.currentDocID
    def currentScore: Float = segmentIterator.currentScore * IDF

    def blockMaxDocID = _blockMaxDocID
    def blockMaxScore = _blockMaxScore
    def segmentMaxDocID = _segmentMaxDocID
    def segmentMaxScore = _segmentMaxScore

    nextSegmentMeta()
    initSegmentIterator()

    def nextBlock() = {
        metaIterator.next()
        block = metaIterator.currentElement
        blockBuffer = ByteBuffer.wrap(block._2.data)
        _blockMaxDocID = block._1
        _blockMaxScore = block._2.maxMeasure.score * IDF
        segmentSkip = 0
    }

    def nextSegmentMeta() = {
        if(!hasNextSegment && hasNextBlock)
            nextBlock()
        if(segmentSkip != 0) {
            blockBuffer.position(blockBuffer.position() + segmentSkip)
        }
        _segmentMaxDocID = blockBuffer.getLong
        _segmentMaxScore = blockBuffer.getFloat * IDF
        //measureSerializer.deserialize(blockBuffer).score
        segmentSkip = blockBuffer.getShort
    }

    def initSegmentIterator() = if(segmentSkip != 0) {
        DocPostingsSerializer.readIntoBuffer(blockBuffer, segmentReadBuffer, encodedLengths)
        maxDocIDsOffset = DocPostingsSerializer.decodeIntoBuffer(segmentReadBuffer,
            measureSerializer.compressFlags,
            segmentDecodedBuffer,
            encodedLengths)

        segmentIterator.maxDocIDOffset = maxDocIDsOffset
        segmentIterator.reset()

        segmentSkip = 0
    }

    def hasNextBlock: Boolean = metaIterator.hasNext
    def hasNextSegment: Boolean = blockBuffer.remaining() > segmentSkip
    def hasNext: Boolean = segmentIterator.hasNext || blockBuffer.remaining() > 0 || metaIterator.hasNext

    def next(): Long = {
        if(segmentIterator.hasNext) segmentIterator.next()
        else {
            if(!hasNextSegment && metaIterator.hasNext)
                nextBlock()
            if(!hasNextSegment) {
                //it will make currentDocID of it == -1
                segmentIterator.next()
            } else {
                nextSegmentMeta()
                initSegmentIterator()
            }
            return currentDocID
        }
    }

    def advanceToScore(targetScore: Float): Long = {
        //in case that segment iterator already processed item with it's max value
        // and we need to jump forward, even thought current segment and block look fine
        while(currentScore < targetScore && hasNext) {
            var blockChanged = false
            //current block might have good MaxScore but it might be empty already
            while ((targetScore > _blockMaxScore || !hasNextSegment)  &&
                metaIterator.hasNext) {
                nextBlock()
                blockChanged = true
            }
            if (blockChanged)
                nextSegmentMeta()
            var segmentChanged = false
            //current segment might have good MaxScore but it might be empty already
            while ((targetScore > _segmentMaxScore || segmentIterator.currentDocID == -1) &&
                hasNextSegment) {
                nextSegmentMeta()
                segmentChanged = true
            }
            if (segmentChanged || blockChanged)
                initSegmentIterator()
            if(_segmentMaxScore >= targetScore || (!hasNextSegment && !hasNextBlock))
                segmentIterator.advance(targetScore / IDF)
        }
        return currentDocID
    }

    def advance(targetDocID: Long): Long = {
        if(targetDocID < currentDocID) {
            val ex = new IllegalStateException(s"wrong use of advance method," +
                " targetDocID < currentDocID")
            logger.error("advance",ex)
            throw ex
        }
        var blockChanged = false
        while (targetDocID > _blockMaxDocID &&  metaIterator.hasNext) {
            metaIterator.advance(targetDocID)
            block = metaIterator.currentElement
            if(block._1 != -1) {
                blockBuffer = ByteBuffer.wrap(block._2.data)
                _blockMaxDocID = block._1
                _blockMaxScore = block._2.maxMeasure.score * IDF
                blockChanged = true
            }
        }
        if(blockChanged)
            nextSegmentMeta()
        var segmentChanged = false
        while (targetDocID > _segmentMaxDocID && blockBuffer.remaining() > segmentSkip) {
            nextSegmentMeta()
            segmentChanged = true
        }
        if(segmentChanged || blockChanged)
            initSegmentIterator()
        segmentIterator.advance(targetDocID)
    }

    def close() = metaIterator.close()
}
