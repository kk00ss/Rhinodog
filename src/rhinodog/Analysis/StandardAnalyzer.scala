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


import java.io.StringReader

import org.apache.lucene.analysis.standard.StandardTokenizer
import rhinodog.Core.Definitions._
import BaseTraits.IAnalyzer
import Configuration.MainComponents
import rhinodog.Core.MeasureFormats.OkapiBM25Measure
import rhinodog.Core.Utils._

import scala.collection._

class StandardAnalyzer() extends IAnalyzer {
    val analyzerName = "StandardAnalyzer"

    val source = new StandardTokenizer()

    override def analyze(doc: Document, lexicon: LocalLexicon): AnalyzedDocument = {
        source.setReader(new StringReader(doc.text))
        source.reset()
        while (source.incrementToken()) {
            val token = source.getAttributeImplsIterator.next().toString
            val lemma = token.toLowerCase.filter(_.isLetter)
            lexicon.addWord(lemma)
        }
        val terms = lexicon.root2TokenInfo.values.toArray.map(token => {
            val measure = OkapiBM25Measure(token.frequency.asInstanceOf[Byte], lexicon.WordsAdded)
            DocTerm(token.ID, measure)
        })
        AnalyzedDocument(doc.text, terms, doc.docMetadata)
    }

}

