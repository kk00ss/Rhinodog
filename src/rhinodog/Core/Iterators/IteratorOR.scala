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

class IteratorOR(components: Seq[ITermIterator],
                 estimateUnit: Float = 0f,
                 combineEstimates: (Float, Float) => Float
                 = (a, b) => a + b) extends ITermIterator {

    var positionChanged = true
    var _currentDocID: Long = -2
    var _currentScore: Float = -2

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

    def hasNextBlock: Boolean = if(_currentDocID != -1) {
        var minComponent = components.head
        for(c <- components.tail)
            if(c.currentDocID != -1 && c.segmentMaxDocID < minComponent.segmentMaxDocID)
                minComponent = c
        minComponent.hasNextBlock
    } else false

    def hasNextSegment: Boolean = if(_currentDocID != -1) {
        var minComponent = components.head
        for(c <- components.tail)
            if(c.currentDocID != -1 && c.segmentMaxDocID < minComponent.segmentMaxDocID)
                minComponent = c
        minComponent.hasNextSegment
    } else false


    def hasNext: Boolean = if(_currentDocID != -1) {
        val ret = components.exists(_.currentDocID != -1)
        if(!ret) _currentDocID = -1
        ret
    } else false

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

    var movedComponents = ArrayBuffer[ITermIterator]()

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
            } else if(!hasNextBlock && !hasNextSegment) {
                positionChanged = true
                _currentDocID = -1
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
        if(hasNext) {
            for (i <- components.indices)
                if (components(i).currentDocID == _currentDocID) {
                    components(i).next()
                }
            positionChanged = true
            updateSmallestPosition()
        }
        return _currentDocID
    }

    //    def advance(targetScore: Float): Long = {
    //        while (targetScore > currentScore)
    //            next()
    //        _currentDocID
    //    }

    def advance(targetDocID: Long): Long = {
        if (hasNext && targetDocID != _currentDocID) {
            components.foreach(_.advance(targetDocID))
            positionChanged = true
            updateSmallestPosition()
        }
        _currentDocID
    }
}
