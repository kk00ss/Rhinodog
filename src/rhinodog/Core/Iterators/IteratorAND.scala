package rhinodog.Core.Iterators

import rhinodog.Core.Definitions.BaseTraits.TermIteratorBase

import scala.collection.mutable.ArrayBuffer

class IteratorAND(components: Seq[TermIteratorBase],
                  estimateUnit: Float = 0f,
                  combineEstimates: (Float, Float) => Float
                  = (a, b) => a + b) extends TermIteratorBase {

    def currentDocID: Long = _currentDocID

    def currentScore: Float = {
        if (positionChanged) {
            computeScore()
        }
        _currentScore
    }

    var positionChanged = true
    var _currentDocID: Long = -1
    var _currentScore: Float = -1

    def computeScore() = {
        _currentScore = components.map(_.currentScore).fold(estimateUnit)(combineEstimates)
        positionChanged = false
    }

    levelIterators()

    def hasNextBlock: Boolean = components.minBy(_.blockMaxDocID).hasNextBlock

    def hasNextSegment: Boolean = components.minBy(_.segmentMaxDocID).hasNextSegment

    //true means there are probably some values left, false means there is nothing left for sure
    def hasNext: Boolean = components.forall(_.currentDocID != -1)

    def areComponentsPositionsEqual: Boolean = {
        val firstPosition = components.head.currentDocID
        var areEqual = true
        var i = 1
        while (areEqual && i < components.length) {
            if (components(i).currentDocID != firstPosition)
                areEqual = false
            i += 1
        }
        return areEqual
    }

    def levelIterators(): Long = {
        if (hasNext) {
            var lastPosition = components.maxBy(_.currentDocID).currentDocID
            var equalityFlag = areComponentsPositionsEqual
            while (!equalityFlag && hasNext) {
                components.foreach(it =>
                    if (it.currentDocID != lastPosition) {
                        val position = it.advance(lastPosition)
                        if (position > lastPosition)
                            lastPosition = position
                    })
                equalityFlag = areComponentsPositionsEqual
            }
            _currentDocID = if (equalityFlag) lastPosition else -1
            return _currentDocID
        } else {
            _currentDocID = -1
            return _currentDocID
        }
    }

    def next(): Long = {
        if (hasNext) {
            positionChanged = true
            val componentWithSmallestPosition = components.minBy(_.currentDocID)
            componentWithSmallestPosition.next()
            levelIterators()
        } else _currentDocID = -1
        return _currentDocID
    }

    def blockMaxDocID: Long = components.minBy(_.blockMaxDocID).blockMaxDocID

    def blockMaxScore: Float = components.map(_.blockMaxScore).sum

    def segmentMaxDocID: Long = components.minBy(_.segmentMaxDocID).blockMaxDocID

    def segmentMaxScore: Float = components.map(_.segmentMaxScore).sum

    //either readSegmentMeta or nextBlock was called on component iterator with this index
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
            positionChanged = true
            var blockChanged = false
            while ((targetScore > blockMaxScore) && hasNextBlock) {
                nextBlock()
                println("block skip - score - andIterator")
                blockChanged = true
            }
            if (blockChanged)
                nextSegmentMeta()
            var segmentChanged = false
            while ((targetScore > segmentMaxScore) && hasNextSegment) {
                nextSegmentMeta()
                println("segment skip - score - andIterator")
                segmentChanged = true
            }
            if (segmentChanged || blockChanged)
                initSegmentIterator()

            if (segmentMaxScore >= targetScore) {
                if (segmentChanged || blockChanged)
                    levelIterators()
                var exitFlag = false
                var blockEnd = blockMaxDocID
                while (!exitFlag && targetScore > currentScore && currentDocID != -1) {
                    next()
                    if (currentDocID > blockEnd) {
                        if (blockMaxScore < targetScore) exitFlag = true
                        else blockEnd = blockMaxDocID
                    }
                }
            }
        }
        return currentDocID
    }

    def advance(targetDocID: Long): Long = {
        if (targetDocID > _currentDocID) {
            components.foreach(_.advance(targetDocID))
            //println("advance - docID - andIterator")
            positionChanged = true
            levelIterators()
        }
        _currentDocID
    }
}
