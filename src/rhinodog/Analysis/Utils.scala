package rhinodog.Analysis

import dk.dren.hunspell.Hunspell
import org.apache.commons.collections4.trie.PatriciaTrie
import scala.collection.JavaConverters._

import scala.collection.mutable

object Utils {
    val dictionary = "en_US"
    private val prefix = System.getProperty("user.dir") + "/hunspell/"
    val speller = Hunspell.getInstance().getDictionary(prefix + dictionary)

    //TODO: add cache
    def getRoots(word: String): mutable.Buffer[String] = {
        val result = speller.stem(word).asScala.filter(_.length > 1)
        if (result.isEmpty)
            result += word
        result
    }

    val stopWordsList = new PatriciaTrie[String](
        scala.io.Source.fromFile("stopWordsList.txt").getLines()
            .toArray
            .map(x => x -> x)
            .toMap.asJava)
}