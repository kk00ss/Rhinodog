//// Copyright 2016 Konstantin Voznesenskiy
//
//// Licensed under the Apache License, Version 2.0 (the "License");
//// you may not use this file except in compliance with the License.
//// You may obtain a copy of the License at
//
////    http://www.apache.org/licenses/LICENSE-2.0
//
//// Unless required by applicable law or agreed to in writing, software
//// distributed under the License is distributed on an "AS IS" BASIS,
//// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//// See the License for the specific language governing permissions and
//// limitations under the License.
//package rhinodog.Analysis
//
//import edu.emory.mathcs.nlp.tokenization.EnglishTokenizer
//
//import rhinodog.Core.Definitions._
//import BaseTraits.IAnalyzer
//import Configuration._
//import rhinodog.Core.MeasureFormats.OkapiBM25Measure
//import rhinodog.Core.Utils._
//
//import scala.collection.JavaConverters._
//import scala.collection._
//
//
//class AnalyzerWithShingles(dependencies: MainComponents) extends IAnalyzer {
//    val analyzerName = "AnalyzerWithShingles"
//
//    case class Shingle
//    (ids: Seq[Int],
//     tokenInfo: TokenInfo,
//     importance: Float)
//
//    val tokenizer = new EnglishTokenizer()
//
//    override def analyze(doc: Document,
//                         config: mutable.Map[String,String]
//                         = mutable.Map[String, String]()): AnalyzedDocument = {
//
//        val shinglesMaxRatio = getConfigParam(config, "shinglesMaxRatio", 1f, _.toFloat)
//        val maxDistanceBetweenWords = getConfigParam(config,"maxDistanceBetweenWords", 10, _.toInt)
//        val generateShingles = getConfigParam(config, "generateShingles", true, _.toBoolean)
//        //more important for smaller texts
//        val useHunspell = getConfigParam(config, "useHunspell", true, _.toBoolean)
//
//        val tokens = tokenizer.segmentize(doc.text).asScala
//        val lexicon = new LocalLexicon(dependencies.repository, useHunspell)
//        var words4word = if(generateShingles) SortedMap[(Int, Int), Float]() else null
//
//        var maxConnectedness: Float = 0f
//        for (i <- tokens.indices) {
//            val parsedSentence = tokens(i).asScala
//            val wordIds = new Array[Array[Int]](parsedSentence.length)
//            for (j <- parsedSentence.indices) {
//                val lemma = parsedSentence(j).getWordForm
//                    .toLowerCase
//                    .filter(_.isLetter)
//                if (lemma.length > 0) {
//                    val currentWordIDs = lexicon.addWord(lemma)
//                    if (currentWordIDs != null && generateShingles) {
//                        //initializing words4word
//                        for (wid <- currentWordIDs) {
//                            for (i <- 0 until j) {
//                                val prevPositionsWordIDs = wordIds(i)
//                                if (prevPositionsWordIDs != null)
//                                    for (prevWordID <- prevPositionsWordIDs)
//                                        if (prevWordID > wid) {
//                                            val distance = j - i
//                                            if (distance <= maxDistanceBetweenWords) {
//                                                val key = (wid, prevWordID)
//                                                val prevValue = words4word.getOrElse(key, 0f)
//                                                val newValue = 1f / distance + prevValue
//                                                words4word += ((key,newValue))
//                                                if (newValue > maxConnectedness)
//                                                    maxConnectedness = newValue
//                                            }
//                                        }
//                            }
//                        }
//                        for (i <- 0 until j) {
//                            val prevPositionsWordIDs = wordIds(i)
//                            if (prevPositionsWordIDs != null)
//                                for (prevWordID <- prevPositionsWordIDs) {
//                                    for (wid <- currentWordIDs)
//                                        if (prevWordID < wid) {
//                                            val distance = j - i
//                                            if (distance <= maxDistanceBetweenWords) {
//                                                val key = (prevWordID, wid)
//                                                val prevValue = words4word.getOrElse(key, 0f)
//                                                val newValue = 1f / distance + prevValue
//                                                words4word += ((key,newValue))
//                                                if (newValue > maxConnectedness)
//                                                    maxConnectedness = newValue
//                                            }
//                                        }
//                                }
//                        }
//
//                        wordIds(j) = currentWordIDs
//                    }
//                }
//            }
//        }
//        var tokensRet =  lexicon.root2TokenInfo.values
//
//        if(generateShingles) {
//            val numberOfShinglesToReturn = lexicon.root2TokenInfo.size * shinglesMaxRatio
//            var statShinglesBigrams = new mutable.PriorityQueue[(Float, Shingle)]() (Ordering.by(x => x._2.importance * -1))
//
//            def pow(num: Float, power: Int): Float = {
//                var acc = num
//                var currentPower = 1
//                while (currentPower < power) {
//                    acc *= num
//                    currentPower += 1
//                }
//                acc
//            }
//
//            words4word.foreach(keyVal => {
//                val word1 = lexicon.root2TokenInfo(keyVal._1._1)
//                if (word1.frequency > 1) {
//                    val word2 = lexicon.root2TokenInfo(keyVal._1._2)
//                    if (word2.frequency > 1) {
//
//                        val connectedness = keyVal._2
//                        val avgFrequency = math.sqrt(word1.frequency * word2.frequency)
//                                            .asInstanceOf[Float]
//
//                        val estimatedImportance = pow(connectedness, 10) * avgFrequency
//                        if (statShinglesBigrams.size < numberOfShinglesToReturn
//                            || statShinglesBigrams.head._1 < estimatedImportance) {
//                            val tmp = List(word1, word2).sortBy(_.ID)
//                            val termString = tmp.map(_.rootForm).mkString("%")
//
//                            val shingleTermID = dependencies.repository.getTermID(termString)
//                            statShinglesBigrams += ((estimatedImportance,
//                                new Shingle(tmp.map(_.ID),
//                                            TokenInfo(shingleTermID,
//                                                      //termString,
//                                                      math.round(avgFrequency)),
//                                            estimatedImportance)))
//                            if (statShinglesBigrams.size > numberOfShinglesToReturn)
//                                statShinglesBigrams.dequeue()
//                        }
//                    }
//                }
//            })
//
//            tokensRet ++= statShinglesBigrams.unzip._2.map(_.tokenInfo)
////                    val shinglesRet2 = statShinglesBigrams.unzip._2.toArray
////                      .sortWith(_.importance > _.importance)
////                    shinglesRet2.map(println)
//        }
//        val terms = tokensRet.map(token => {
//            val measure = OkapiBM25Measure(token.frequency.asInstanceOf[Byte], lexicon.WordsAdded)
//            DocTerm(token.ID, measure)
//        }).toSeq
//        AnalyzedDocument(doc.text, terms, doc.docMetadata)
//    }
//
//}
