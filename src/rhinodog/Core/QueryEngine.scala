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

import scala.collection.mutable

import Definitions._
import BaseTraits._
import Configuration._

import rhinodog.Core.Iterators._


sealed trait BooleanClause
case class ElementaryClause(termID: Int)extends BooleanClause
case class ANDClause(terms: Seq[BooleanClause]) extends BooleanClause
case class ORClause(terms: Seq[BooleanClause]) extends BooleanClause

class QueryEngine
(dependencies: MainComponents,
 storage: Storage,
 estimateUnit: Float = 0f,
 combineEstimates: (Float, Float) => Float = (a, b) => a + b) {

    val floatMinMeaningfulValue = 0.000001f

    //this is iterator for querying persistent data
    def buildTopLevelIterator(query: BooleanClause): ITermIterator = {
        val snapshotReader = dependencies.repository.getSnapshotReader
        val totalDocs = storage.totalDocsCount
        def getTermIterator(termID: Int) = {
            val meta = dependencies.metadataManager.getTermMetadata(termID)
            if(meta.isEmpty)
                throw new IllegalArgumentException("unable to create Metadata Iterator")
            else {
                val metaIterator = new MetaIterator(termID, meta.get, snapshotReader)
                val termFrequency = meta.get.numberOfDocs
                new BlocksIterator(metaIterator,
                                   dependencies.measureSerializer,
                                   termFrequency,
                                   totalDocs)
            }
        }
        def mapClauseToIterator(clause: BooleanClause): ITermIterator
        = clause match {
            case ElementaryClause(termID) => getTermIterator(termID)
            case ANDClause(children) => new IteratorAND(children.map(mapClauseToIterator),
                                                        estimateUnit,
                                                        combineEstimates)
            case ORClause(children) => new IteratorOR(children.map(mapClauseToIterator),
                                                      estimateUnit,
                                                      combineEstimates)
        }
        val topLevelIterator = mapClauseToIterator(query)
        topLevelIterator
    }

    def executeQuery(topLevelIterator: ITermIterator, K: Int): mutable.PriorityQueue[(Float, Long)] ={
        val topK = new mutable.PriorityQueue[(Float, Long)]() (Ordering.by(_._1 * -1))
        var topKPopulated = false
        while (topLevelIterator.currentDocID != -1) {
            if ((topK.size < K || topLevelIterator.currentScore > topK.head._1)
                && !dependencies.metadataManager.isDeleted(topLevelIterator.currentDocID)) {
                topK += ((topLevelIterator.currentScore, topLevelIterator.currentDocID))
                if (topK.size > K) {
                    topK.dequeue()
                    topKPopulated = true
                }
            }
            topLevelIterator.next()
            if (topKPopulated)
                topLevelIterator.advanceToScore(topK.head._1 + floatMinMeaningfulValue)
        }
        topK
    }
}
