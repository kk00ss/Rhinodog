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

import scala.collection._

class RowSegmentIterator
(data: Seq[DocPosting]) {
    private var position = 0
    private var changed = true
    private var score = 0f
    updateState

    def advanceToScore(targetScore: Float): Long = {
        while (hasNext && currentScore < targetScore)
            next()
        if (hasNext || currentScore >= targetScore) return currentDocID
        else return -1
    }

    def currentDocID = data(position).docID

    def currentScore = if (!changed) score
    else {
        updateState
        score
    }

    private def updateState = if (position < data.size) {
        val current = data(position)
        score = current.measure.score
        changed = false
    }
    else throw new IllegalStateException("not defined at this position")

    def next(): DocPosting = {
        if (hasNext) {
            position += 1
            changed = true
            return data(position)
        } else return null
    }

    def hasNext = position + 1 < data.size

    def advance(docID: Long): Long = {
        var step = 1
        while (position + step < data.length && currentDocID < docID) {
            position += step
            step *= 2
            changed = true
        }
        if (currentDocID > docID && step > 2)
            while (data(position - 1).docID >= docID)
                position -= 1
        if (hasNext || currentDocID >= docID) return currentDocID
        else return -1
    }
}
