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

import rhinodog.Core.Definitions._

class SerializedSegmentIterator
(data: SegmentSerialized,
 measureSerializer: MeasureSerializerBase,
 _maxDocIDOffset: Int = 128) {

    var maxDocIDOffset = _maxDocIDOffset
    var docIDposition = 0
    var docIDstep = 0
    var measurePosition = 0
    var currentDocID = 0l
    private var changed = true
    private var _currentScore = 0f

    computeCurrentDocID()
    computeScore()

    def reset() = {
        docIDposition = 0
        docIDstep = 0
        measurePosition = 0
        currentDocID = 0l
        changed = true
        _currentScore = 0f
        computeCurrentDocID()
        computeScore()
    }

    def currentScore = if (!changed) _currentScore
    else {
        computeScore()
        changed = false
        //println(_currentScore)
        _currentScore
    }


    private def computeScore() = if (currentDocID != -1)
        _currentScore = measureSerializer.scoreFromComponents(data, measurePosition)
    else _currentScore = -1f

    private def computeCurrentDocID() = {
        var docID = currentDocID
        if (docIDposition >= maxDocIDOffset)
            currentDocID = -1
        else {
            changed = true
            if (data(0)(docIDposition) != 0) {
                docID += data(0)(docIDposition)
                docIDstep = 1
            } else {
                docID += data(0)(docIDposition + 1) *
                    Int.MaxValue.asInstanceOf[Long] +
                    data(0)(docIDposition + 2)
                docIDstep = 3
            }
            currentDocID = docID
        }
    }

    def hasNext: Boolean = docIDposition + docIDstep < maxDocIDOffset

    def next(): Long = {
        docIDposition += docIDstep
        measurePosition += 1
        computeCurrentDocID()
        return currentDocID
    }

    def advance(targetScore: Float): Long = {
        while (currentDocID != -1 && currentScore < targetScore)
            next()
        return currentDocID
    }

    def advance(docID: Long): Long = {
        while (currentDocID != -1 && currentDocID < docID) {
            next()
        }
        return currentDocID
    }
}
