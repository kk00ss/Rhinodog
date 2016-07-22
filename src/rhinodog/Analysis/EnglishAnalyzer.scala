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
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.netflix.config.DynamicPropertyFactory
import org.apache.lucene.analysis._
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.util.Version
import org.slf4j.LoggerFactory
import rhinodog.Core.Definitions.BaseTraits.IAnalyzer
import rhinodog.Core.Definitions._
import rhinodog.Core.MeasureFormats.{BMLikeMeasure, OkapiBM25Measure}
import scala.collection.JavaConverters._

class EnglishAnalyzer() extends IAnalyzer {
    private val logger = LoggerFactory.getLogger(this.getClass)

    val analyzerName = "EnglishAnalyzer"
    private val analyzerLock = new ReentrantReadWriteLock()

    val fileNamePropery = DynamicPropertyFactory.getInstance()
        .getStringProperty("analysis.stopWordsList.English",
            null,
            new Runnable {
                override def run() = {
                    val tmpAnalyzer = initAnalyzer()
                    analyzerLock.writeLock().lock()
                    try {
                        englishAnalyzer = tmpAnalyzer
                    } finally {
                        analyzerLock.writeLock().unlock()
                    }
                }
            })

    var englishAnalyzer: en.EnglishAnalyzer = initAnalyzer()

    private def initAnalyzer(): en.EnglishAnalyzer = {
        if (fileNamePropery.get() != null) {
            val path = fileNamePropery.get()
            val file = new java.io.File(path)
            if (!file.exists()) {
                val ex = new IllegalArgumentException(s"EnglishAnalyszer incorrect path for stopwordsList path = $path")
                logger.error("Continuing without stopWordsList", ex)
                new en.EnglishAnalyzer()
            } else {
                val stopList = scala.io.Source.fromFile(file)
                    .getLines().toSeq.asJava
                val charArraySet = new CharArraySet(stopList, true)
                new en.EnglishAnalyzer(charArraySet)

            }
        } else new en.EnglishAnalyzer()
    }

    override def analyze(doc: Document, lexicon: LocalLexicon): AnalyzedDocument = {
        //        analyzerLock.readLock().lock()
        //        try {
        val in = new StringReader(doc.text)
        val tokenStream = englishAnalyzer.tokenStream("all", in)

        val termAtt = tokenStream.addAttribute(classOf[CharTermAttribute])
        tokenStream.reset()
        var totalTokenCount = 0
        while (tokenStream.incrementToken()) {
            totalTokenCount += 1
            val termBuff = termAtt.buffer()
            val termLen = termAtt.length()
            val token = new String(termBuff, 0, termLen)
            val lemma = token.toLowerCase //.filter(_.isLetter)
            lexicon.addWord(lemma)
        }
        tokenStream.end()
        tokenStream.close()
        val terms = lexicon.root2TokenInfo.values.toArray.map(token => {
            val measure = BMLikeMeasure(token.frequency.asInstanceOf[Short], totalTokenCount)
            DocTerm(token.ID, measure)
        })
        AnalyzedDocument(doc.ID, terms)
        //        } finally {
        //            analyzerLock.readLock().unlock()
        //        }
    }

}

