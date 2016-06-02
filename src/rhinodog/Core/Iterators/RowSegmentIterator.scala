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

