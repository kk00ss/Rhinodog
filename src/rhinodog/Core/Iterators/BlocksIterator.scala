package rhinodog.Core.Iterators

import java.nio.ByteBuffer

import rhinodog.Core.Definitions._
import BaseTraits.TermIteratorBase
import rhinodog.Core.Utils.DocPostingsSerializer

//TODO: add softNot and strongNot which would lower estimate for documents

class BlocksIterator
(metaIterator: MetadataIteratorBase,
 measureSerializer: MeasureSerializerBase,
 termFrequency: Long,
 totalDocs: Long) extends TermIteratorBase {
    if(termFrequency == 0)
        throw new IllegalArgumentException("termFrequency cannot be 0, " +
            "term should be excluded from query")

    val IDF: Float = computeIDF

    private def computeIDF: Float = {
        val tmp = math.log((totalDocs - termFrequency + 0.5) / (termFrequency + 0.5))
                .asInstanceOf[Float]
        if(tmp < 0) return 0
        else tmp
    }

    private var block:  (Long, BlockInfoBase) = metaIterator.currentElement
    private var blockBuffer: ByteBuffer = ByteBuffer.wrap(block._2.data)
    private var _blockMaxDocID: Long = block._1
    private var _blockMaxScore: Float = block._2.maxMeasure.score

    val numComponents = measureSerializer.numberOfComponentsRequired + 1
    var maxDocIDsOffset = 0 //uninitialized value

    val segmentReadBuffer = new SegmentSerialized(numComponents)
    val encodedLengths = new Array[Int](numComponents)
    val segmentDecodedBuffer = new SegmentSerialized(numComponents)

    //large longs can take up to 3 Int's to encode (0 for a flag)
    segmentReadBuffer(0) = new Array[Int](DocPostingsSerializer.segmentSize * 3)
    segmentDecodedBuffer(0) = new Array[Int](DocPostingsSerializer.segmentSize * 3)
    for(i <- 1 to measureSerializer.numberOfComponentsRequired) {
        segmentReadBuffer(i) = new Array[Int](DocPostingsSerializer.segmentSize)
        segmentDecodedBuffer(i) = new Array[Int](DocPostingsSerializer.segmentSize)
    }

    private var _segmentMaxDocID: Long = 0
    private var _segmentMaxScore: Float = 0
    private var segmentSkip: Short = 0
    private val segmentIterator = new SerializedSegmentIterator(segmentDecodedBuffer,
        measureSerializer,
        maxDocIDsOffset)

    def currentDocID: Long = segmentIterator.currentDocID
    def currentScore: Float = segmentIterator.currentScore

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
        _blockMaxScore = block._2.maxMeasure.score
    }

    def nextSegmentMeta() = {
        if(segmentSkip != 0) {
            blockBuffer.position(blockBuffer.position() + segmentSkip)
        }
        _segmentMaxDocID = blockBuffer.getLong
        _segmentMaxScore =  measureSerializer.deserialize(blockBuffer).score
        segmentSkip = blockBuffer.getShort
    }

    def initSegmentIterator() = if(segmentSkip != 0) {
        DocPostingsSerializer.readIntoBuffer(blockBuffer, segmentReadBuffer, encodedLengths)
        maxDocIDsOffset = DocPostingsSerializer.decodeIntoBuffer(segmentReadBuffer,
            segmentDecodedBuffer,
            encodedLengths)

        segmentIterator.maxDocIDOffset = maxDocIDsOffset
        segmentIterator.reset()

        segmentSkip = 0
    }

    def hasNextBlock: Boolean = metaIterator.hasNext
    def hasNextSegment: Boolean = blockBuffer.remaining() > 0
    def hasNext: Boolean = segmentIterator.hasNext || blockBuffer.remaining() > 0 || metaIterator.hasNext

    def next(): Long = {
        if(segmentIterator.hasNext) segmentIterator.next()
        else {
            if(blockBuffer.remaining() == 0 && metaIterator.hasNext)
                nextBlock()
            if(blockBuffer.remaining() == 0) {
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
            while ((targetScore > _blockMaxScore || blockBuffer.remaining() == 0)  &&
                metaIterator.hasNext) {
                nextBlock()
                //println("block skip - score - blockIterator")
                blockChanged = true
            }
            if (blockChanged)
                nextSegmentMeta()
            var segmentChanged = false
            //current segment might have good MaxScore but it might be empty already
            while ((targetScore > _segmentMaxScore || segmentIterator.currentDocID == -1) &&
                blockBuffer.remaining() > segmentSkip) {
                nextSegmentMeta()
                //println("segment skip - score - blockIterator")
                segmentChanged = true
            }
            if (segmentChanged || blockChanged)
                initSegmentIterator()
            if(_segmentMaxScore >= targetScore)
                segmentIterator.advance(targetScore)
        }
        return currentDocID
    }

    def advance(targetDocID: Long): Long = {
        if(targetDocID < currentDocID)
            throw new IllegalStateException(s"wrong use of advance method," +
                s" targetDocID = $targetDocID currentDocID = $currentDocID")
        var blockChanged = false
        while (targetDocID > _blockMaxDocID &&  metaIterator.hasNext) {
            //nextBlock()
            metaIterator.advance(targetDocID)
            block = metaIterator.currentElement
            blockBuffer = ByteBuffer.wrap(block._2.data)
            _blockMaxDocID = block._1
            _blockMaxScore = block._2.maxMeasure.score
            //            if(targetDocID > _blockMaxDocID )
            //            println("block skip - docID - blockIterator")
            blockChanged = true
        }
        if(blockChanged)
            nextSegmentMeta()
        var segmentChanged = false
        while (targetDocID > _segmentMaxDocID && blockBuffer.remaining() > segmentSkip) {
            nextSegmentMeta()
            //            if(targetDocID > _segmentMaxDocID)
            //            println("segment skip - docID - blockIterator")
            segmentChanged = true
        }
        if(segmentChanged || blockChanged)
            initSegmentIterator()
        //if(_segmentMaxDocID >= targetDocID)
        segmentIterator.advance(targetDocID)
        //else -1
    }
}
