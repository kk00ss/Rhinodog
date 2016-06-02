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
package rhinodog.Analysis

import edu.emory.mathcs.nlp.tokenization.EnglishTokenizer
import rhinodog.Core.Definitions.BaseTraits.Analyzer
import rhinodog.Core.Definitions.Configuration.MainComponents
import rhinodog.Core.Definitions.{AnalyzedDocument, BaseTraits, DocTerm}
import rhinodog.Core.MeasureFormats.MeasureSimple
import rhinodog.Core.Utils._

import scala.collection.JavaConverters._
import scala.collection._

case class TokenInfo
(ID: Int,
 rootForm: String,
 var frequency: Int)

class AnalyzerForMeasureSimple(dependencies: MainComponents) extends Analyzer {
    val analyzerName = "AnalyzerForMeasureSimple"

    case class Shingle
    (ids: Seq[Int],
     tokenInfo: TokenInfo,
     importance: Float)

    val tokenizer = new EnglishTokenizer()

    override def analyze(text: String,
                         docMetadata: mutable.Map[String, String]
                         = mutable.Map[String, String](),
                         config: mutable.Map[String,String]
                         = mutable.Map[String, String]()): AnalyzedDocument = {

        //more important for smaller texts
        val useHunspell = getConfigParam(config, "useHunspell", true, _.toBoolean)

        val tokens = tokenizer.segmentize(text).asScala
        val lexicon = new LocalLexicon(dependencies.repository, useHunspell)
        for (i <- tokens.indices) {
            val parsedSentence = tokens(i).asScala
            for (j <- parsedSentence.indices) {
                val lemma = parsedSentence(j).getWordForm
                    .toLowerCase
                    .filter(_.isLetter)
                if (lemma.length > 0) {
                    lexicon.addWord(lemma)
                }
            }
        }
        var tokensRet =  lexicon.idToRoot.values
        val terms = tokensRet.map(token => {
            val measure = MeasureSimple(token.frequency.asInstanceOf[Byte], lexicon.WordsAdded)
            DocTerm(token.ID, measure)
        }).toSeq
        AnalyzedDocument(text, terms, docMetadata)
    }

}
