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
