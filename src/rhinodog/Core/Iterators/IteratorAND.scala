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

import rhinodog.Core.Definitions.BaseTraits.ITermIterator

import scala.collection.mutable.ArrayBuffer

class IteratorAND(components: Seq[ITermIterator],
                  estimateUnit: Float = 0f,
                  combineEstimates: (Float, Float) => Float
                  = (a, b) => a + b) extends ITermIterator {

    def currentDocID: Long = _currentDocID

    def currentScore: Float = {
        if (positionChanged) {
            computeScore()
        }
        _currentScore
    }

    var positionChanged = true
    var _currentDocID: Long = -2
    var _currentScore: Float = -2

    def computeScore() = {
        _currentScore = estimateUnit
        for(c <- components)
            _currentScore = combineEstimates(_currentScore, c.currentScore)
        positionChanged = false
    }

    levelIterators()

    def hasNextBlock: Boolean = components.minBy(_.blockMaxDocID).hasNextBlock

    def hasNextSegment: Boolean = components.minBy(_.segmentMaxDocID).hasNextSegment

    //true means there are probably some values left, false means there is nothing left for sure
    def hasNext: Boolean = if(_currentDocID != -1) {
        val ret = components.forall(_.currentDocID != -1)
        if(!ret) _currentDocID = -1
        ret
    } else false

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
        }
        return _currentDocID
    }

    def next(): Long = {
        if (hasNext) {
            positionChanged = true
            val componentWithSmallestPosition = components.minBy(_.currentDocID)
            componentWithSmallestPosition.next()
            levelIterators()
        }
        return _currentDocID
    }

    def blockMaxDocID: Long = components.minBy(_.blockMaxDocID).blockMaxDocID

    def blockMaxScore: Float = {
        var ret = estimateUnit
        components.foreach(c => ret = combineEstimates(c.blockMaxScore, ret))
        ret
    }

    def segmentMaxDocID: Long = components.minBy(_.segmentMaxDocID).blockMaxDocID

    def segmentMaxScore: Float = {
        var ret = estimateUnit
        components.foreach(c => ret = combineEstimates(c.segmentMaxScore, ret))
        ret
    }

    // nextSegmentMeta was called on component iterators
    var movedSegments = ArrayBuffer[ITermIterator]()

    // nextBlock was called on component iterators
    var movedBlocks = ArrayBuffer[ITermIterator]()

    def nextBlock() = {
        val componentToMove = components.minBy(_.blockMaxDocID)
        if(componentToMove.hasNextBlock) {
            componentToMove.nextBlock()
            componentToMove.nextSegmentMeta()
            if (!movedBlocks.contains(componentToMove))
                movedBlocks += componentToMove
        } else _currentDocID = -1
    }

    def nextSegmentMeta() = {
        val componentToMove = components.minBy(_.segmentMaxDocID)
        if(componentToMove.hasNextSegment) {
            componentToMove.nextSegmentMeta()
            if (!movedSegments.contains(componentToMove))
                movedSegments += componentToMove
        } else _currentDocID = -1
    }

    def initSegmentIterator() = {
        movedSegments.foreach(_.initSegmentIterator())
        movedSegments.clear()
    }

    def advanceToScore(targetScore: Float): Long = {
        //in case that segment iterator already processed item with it's max value
        // and we need to jump forward, even thought current segment and block look fine
        // or block estimate was too optimistic and we were unable to find proper element
        while (currentScore < targetScore && hasNext) {
            positionChanged = true
            var blockChanged = false
            while (hasNextBlock &&
                ( (targetScore > blockMaxScore)
                    || (!hasNextSegment && segmentMaxScore < targetScore) )) {
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
            } else if(!hasNextBlock && !hasNextSegment) {
                positionChanged = true
                _currentDocID = -1
            }
        }
        return currentDocID
    }

    def advance(targetDocID: Long): Long = {
        if (hasNext && targetDocID > _currentDocID) {
            components.foreach(_.advance(targetDocID))
            positionChanged = true
            levelIterators()
        }
        _currentDocID
    }

    def close() = components.foreach(_.close())
}
