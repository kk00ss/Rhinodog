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

import org.slf4j.LoggerFactory
import rhinodog.Core.Utils.DocumentSerializer

import scala.StringBuilder
import scala.collection.mutable

import Definitions._
import BaseTraits._
import Configuration._

import rhinodog.Core.Iterators._


sealed trait BooleanClause
case class TermID(termID: Int)extends BooleanClause
case class TermToken(term: String)extends BooleanClause
case class ANDClause(terms: Seq[BooleanClause]) extends BooleanClause
case class ORClause(terms: Seq[BooleanClause]) extends BooleanClause

class QueryEngine
(dependencies: MainComponents,
 storage: Storage,
 estimateUnit: Float = 0f,
 combineEstimates: (Float, Float) => Float = (a, b) => a + b) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    val floatMinMeaningfulValue = 0.000001f

    private val _buildIteratorTimer = dependencies.metrics.timer("Build Composite Iterator")
    private val _executeQueryTimer = dependencies.metrics.timer("Execute Query")

    private def mapTermToID(clause: BooleanClause): BooleanClause = clause match {
        case TermToken(term) => TermID(dependencies.repository.getTermID(term))
        case ANDClause(children) => new ANDClause(children.map(mapTermToID))
        case ORClause(children) => new ORClause(children.map(mapTermToID))
        case TermID(x) => TermID(x)
    }

    //returns iterator for querying persistent data
    private def buildTopLevelIterator(query: BooleanClause): ITermIterator = {
        val context = _buildIteratorTimer.time()
        val totalDocs = storage.getTotalDocs
        val IDsONLY = mapTermToID(query)
        val snapshotReader = dependencies.repository.getSnapshotReader
        def getTermIterator(termID: Int) = {
            val meta = dependencies.metadataManager.getTermMetadata(termID)
            if(meta.isEmpty) {
                val ex = new IllegalArgumentException("unable to create Metadata Iterator")
                logger.error("!!! buildTopLevelIterator",ex)
                throw ex
            }
            else {
                val metaIterator = new MetaIterator(termID, meta.get, snapshotReader)
                val termFrequency = meta.get.numberOfDocs
                new BlocksIterator(metaIterator,
                    dependencies.measureSerializer,
                    termFrequency,
                    totalDocs,
                    true)
            }
        }
        def mapClauseToIterator(clause: BooleanClause): ITermIterator
        = clause match {
            case TermID(termID) => getTermIterator(termID)
            case ANDClause(children) => new IteratorAND(children.map(mapClauseToIterator),
                                                        estimateUnit,
                                                        combineEstimates)
            case ORClause(children) => new IteratorOR(children.map(mapClauseToIterator),
                                                      estimateUnit,
                                                      combineEstimates)
            case TermToken(_) => {
                val ex = new IllegalArgumentException("token should be converted to tokenID first")
                logger.error("!!! buildTopLevelIterator",ex)
                throw ex
            }
        }
        val topLevelIterator = mapClauseToIterator(IDsONLY)
        context.stop()
        topLevelIterator
    }

    private def executeQuery(topLevelIterator: ITermIterator, K: Int): mutable.PriorityQueue[(Float, Long)] = {
        val context = _executeQueryTimer.time()
        val topK = new mutable.PriorityQueue[(Float, Long)]() (Ordering.by(_._1 * -1))
        var topKPopulated = false
        while (topLevelIterator.currentDocID != -1) {
            if ((topK.size < K || topLevelIterator.currentScore > topK.head._1)
                && !dependencies.metadataManager.isDeleted(topLevelIterator.currentDocID)) {
                logger.trace("executeQuery new match found score = {}, docID = {}",
                            topLevelIterator.currentScore, topLevelIterator.currentDocID)
                topK += ((topLevelIterator.currentScore, topLevelIterator.currentDocID))
                if (topK.size > K) {
                    topK.dequeue()
                    topKPopulated = true
                }
            }
            topLevelIterator.next()
//            if (topKPopulated)
//                topLevelIterator.advanceToScore(topK.head._1)
        }
        topLevelIterator.close()
        context.stop()
        topK
    }

    def executeQuery(queryTopClause: BooleanClause, K: Int = 10): mutable.PriorityQueue[(Float, Long)] = {
        val topLevelIterator = buildTopLevelIterator(queryTopClause)
        val result = executeQuery(topLevelIterator, K)
        result
    }

    def explain(query: BooleanClause, docID: Long): String = {
        val strBldr = new StringBuilder()
        val totalDocs = storage.getTotalDocs
        strBldr.append(s" TotalDocs = $totalDocs \n")
        val serializedDoc = dependencies.repository.getDocument(docID).get
        val analyzedDoc = new DocumentSerializer(dependencies.measureSerializer).deserialize(serializedDoc)
        val terms = analyzedDoc.terms.map(x => x.termID -> x.measure).toMap
        var offset = 0
        def generateOffset(size: Int) = "    "*size
        def explainTerm(termID: Int) = {
            val meta = dependencies.metadataManager.getTermMetadata(termID)
            if(meta.isEmpty) {
                val ex = new IllegalArgumentException("unable to create Metadata Iterator")
                logger.error("!!! buildTopLevelIterator",ex)
                throw ex
            }
            else {

                val termFrequency = meta.get.numberOfDocs
                val IDF = BlocksIterator.computeIDF(totalDocs,termFrequency)
                if(terms.contains(termID)) {
                    val measure = terms(termID)
                    val totalScore = measure.score * IDF
                    strBldr.append(
                        generateOffset(offset) + s" ==== TermID = $termID TermScore =$totalScore \n" +
                            generateOffset(offset) + s" IDF = $IDF <= termFrequency = $termFrequency totalDocs = $totalDocs\n" +
                            generateOffset(offset) + s" Measure score = ${measure.score} value = $measure \n")
                } else strBldr.append(
                    generateOffset(offset) + s" ==== TermID = $termID TermScore =0 \n")
            }
        }
        def explainClause(clause: BooleanClause): Unit
        = clause match {
            case TermID(termID) => explainTerm(termID)
            case ANDClause(children) => {
                strBldr.append(generateOffset(offset))
                strBldr.append(" ==== AND Clause score = sum of \n")
                offset +=1
                children.map(explainClause)
                offset -=1
            }
            case ORClause(children) => {
                strBldr.append(generateOffset(offset))
                strBldr.append(" ==== OR Clause score = sum of \n")
                offset +=1
                children.map(explainClause)
                offset -=1
            }
            case TermToken(_) => {
                val ex = new IllegalArgumentException("token should be converted to tokenID first")
                logger.error("!!! buildTopLevelIterator",ex)
                throw ex
            }
        }
        val IDsONLY = mapTermToID(query)
        explainClause(IDsONLY)
        strBldr.append("\n")
        strBldr.toString()
    }
}
