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

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.netflix.config.DynamicPropertyFactory
import org.apache.lucene.analysis._
import org.apache.lucene.analysis.util.CharArraySet
import rhinodog.Core.Definitions.BaseTraits.IAnalyzer
import rhinodog.Core.Definitions._
import rhinodog.Core.MeasureFormats.OkapiBM25Measure
import scala.collection.JavaConverters._

class EnglishAnalyzer() extends IAnalyzer {
    val analyzerName = "EnglishAnalyzer"
    private val analyzerLock = new ReentrantReadWriteLock()

    val fileNamePropery = DynamicPropertyFactory.getInstance()
        .getStringProperty("analysis.stopWordsList.English",
            null,
            new Runnable { override def run() = {
                val tmpAnalyzer = initAnalyzer()
                analyzerLock.writeLock().lock()
                try {
                    englishAnalyzer = tmpAnalyzer
                } finally { analyzerLock.writeLock().unlock() }
            } })

    var englishAnalyzer: en.EnglishAnalyzer = initAnalyzer()

    private def initAnalyzer(): en.EnglishAnalyzer = {
        if(fileNamePropery.get() != null) {
            val stopList = scala.io.Source.fromFile(fileNamePropery.get())
                                          .getLines().toSeq.asJava
            val charArraySet = new CharArraySet(stopList, true)
            new en.EnglishAnalyzer(charArraySet)
        } else new en.EnglishAnalyzer()
    }

    override def analyze(doc: Document, lexicon: LocalLexicon): AnalyzedDocument = {
        analyzerLock.readLock().lock()
        try {
            val tokenStream = englishAnalyzer.tokenStream("all", doc.text)
            tokenStream.reset()
            while (tokenStream.incrementToken()) {
                val token = tokenStream.getAttributeImplsIterator.next().toString
                val lemma = token.toLowerCase.filter(_.isLetter)
                lexicon.addWord(lemma)
            }
            val terms = lexicon.root2TokenInfo.values.toArray.map(token => {
                val measure = OkapiBM25Measure(token.frequency.asInstanceOf[Byte], lexicon.WordsAdded)
                DocTerm(token.ID, measure)
            })
            AnalyzedDocument(doc.text, terms, doc.docMetadata)
        } finally { analyzerLock.readLock().unlock() }
    }

}

