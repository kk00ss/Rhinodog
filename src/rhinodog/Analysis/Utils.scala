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
