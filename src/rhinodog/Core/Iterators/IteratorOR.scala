package rhinodog.Core.Iterators

import rhinodog.Core.Definitions.BaseTraits.TermIteratorBase

import scala.collection.mutable.ArrayBuffer

class IteratorOR(components: Seq[TermIteratorBase],
                 estimateUnit: Float = 0f,
                 combineEstimates: (Float, Float) => Float
                 = (a, b) => a + b) extends TermIteratorBase {

    var positionChanged = true
    var _currentDocID: Long = -1
    var _currentScore: Float = -1

    updateSmallestPosition()

    def currentDocID: Long = _currentDocID

    def currentScore: Float = {
        if (positionChanged) {
            computeScore()
        }
        _currentScore
    }

    def computeScore() = {
        _currentScore = estimateUnit
        components.foreach(c =>
            if (c.currentDocID == _currentDocID)
                _currentScore = combineEstimates(c.currentScore, _currentScore))
        positionChanged = false
    }

    def hasNextBlock: Boolean = components.exists(_.hasNextBlock)

    def hasNextSegment: Boolean = components.exists(_.hasNextSegment)

    def hasNext: Boolean = components.exists(_.hasNext)

    def blockMaxDocID: Long = components.minBy(_.blockMaxDocID).blockMaxDocID

    def blockMaxScore: Float = components.map(_.blockMaxScore).sum

    def segmentMaxDocID: Long = components.minBy(_.segmentMaxDocID).blockMaxDocID

    def segmentMaxScore: Float = components.map(_.segmentMaxScore).sum

    var movedComponents = ArrayBuffer[TermIteratorBase]()

    def nextBlock() = {
        val componentToMove = components.minBy(_.blockMaxDocID)
        componentToMove.nextBlock()
        componentToMove.nextSegmentMeta()
        if (!movedComponents.contains(componentToMove))
            movedComponents += componentToMove
    }

    def nextSegmentMeta() = {
        val componentToMove = components.minBy(_.segmentMaxDocID)
        componentToMove.nextSegmentMeta()
        if (!movedComponents.contains(componentToMove))
            movedComponents += componentToMove
    }

    def initSegmentIterator() = {
        movedComponents.foreach(_.initSegmentIterator())
        movedComponents.clear()
    }

    def advanceToScore(targetScore: Float): Long = {
        //in case that segment iterator already processed item with it's max value
        // and we need to jump forward, even thought current segment and block look fine
        // or block estimate was too optimistic and we were unable to find proper element
        while (currentScore < targetScore && hasNext) {
            var blockChanged = false
            while ((targetScore > blockMaxScore) && hasNextBlock) {
                nextBlock()
                blockChanged = true
            }
            if (blockChanged)
                nextSegmentMeta()
            var segmentChanged = false
            while ((targetScore > segmentMaxScore) && hasNextSegment) {
                nextSegmentMeta()
                segmentChanged = true
            }
            if (segmentChanged || blockChanged)
                initSegmentIterator()
            if (segmentMaxScore >= targetScore) {
                updateSmallestPosition()
                var exitFlag = false
                while (!exitFlag && targetScore > currentScore
                    /*(currentScore - targetScore) < 0.000002*/ && currentDocID != -1) {
                    next()
                    if (blockMaxScore < targetScore)
                        exitFlag = true
                }
            }

        }
        return currentDocID
    }


    def updateSmallestPosition(): Long = {
        var smallestPosition = Long.MaxValue
        for (i <- components.indices)
            if (components(i).currentDocID != -1 && components(i).currentDocID < smallestPosition)
                smallestPosition = components(i).currentDocID
        //if smallestPosition == 0 it means that all iterators run out of elements
        if (smallestPosition != Long.MaxValue)
            _currentDocID = smallestPosition
        else _currentDocID = -1
        return _currentDocID
    }

    def next(): Long = {
        for (i <- components.indices)
            if (components(i).currentDocID == _currentDocID) {
                components(i).next()
            }
        positionChanged = true
        updateSmallestPosition()
    }

    //    def advance(targetScore: Float): Long = {
    //        while (targetScore > currentScore)
    //            next()
    //        _currentDocID
    //    }

    def advance(targetDocID: Long): Long = {
        if (targetDocID != _currentDocID) {
            components.foreach(_.advance(targetDocID))
            positionChanged = true
            updateSmallestPosition()
        }
        _currentDocID
    }
}

