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

import rhinodog.Core.Definitions.BaseTraits.RepositoryBase
import scala.collection._

//TODO: add reset() so instances could be reused
class LocalLexicon(repository: RepositoryBase,
                   runHunspell: Boolean = false) {
    //cache for Repository getTermID and idToRoot in one hop
    val rootToWord: mutable.Map[String, TokenInfo] = mutable.Map()
    //aka hunspell cache
    val lemmaToRoots: mutable.Map[String, Seq[String]] = mutable.Map()
    val idToRoot: mutable.Map[Int, TokenInfo] = mutable.Map()

    private var wordsAdded = 0
    private var maxFreq = 0f

    def maxFrequency = maxFreq
    def WordsAdded = wordsAdded

    def addWord(lemma: String): Array[Int] = {
        if (lemma.length > 1 && !Utils.stopWordsList.containsKey(lemma)) {
            wordsAdded += 1
            val previousRoots = lemmaToRoots.get(lemma)
            var ret: Array[Int] = null
            if (previousRoots.isDefined)
                ret = UpdateLexicon(lemma, previousRoots.get)
            else {
                var roots = if (runHunspell) Utils.getRoots(lemma)
                            else mutable.Buffer(lemma)
                roots = roots.filterNot(r => r.endsWith("s") &&
                    roots.exists(r2 => r.length == r2.length + 1 && r.startsWith(r2)))
                lemmaToRoots += ((lemma, roots))
                ret = UpdateLexicon(lemma, roots)
            }
            return ret
        }
        return null
    }

    //var localID = 0

    private def UpdateLexicon(lemma: String, roots: Seq[String]): Array[Int] = {
        val ret = new Array[Int](roots.size)
        for (i <- roots.indices) {
            val root = roots(i)
            val previous = rootToWord.get(root)
            if (previous.isDefined) {
                val lexWord = previous.get
                ret(i) = lexWord.ID
                lexWord.frequency += 1
                if (lexWord.frequency > maxFreq)
                    maxFreq = lexWord.frequency
            }
            else {
                val globalID = repository.getTermID(root) //localID //
                //localID += 1
                val newLW = new TokenInfo(globalID, root, 1)
                rootToWord += (root -> newLW)
                idToRoot += (globalID -> newLW)
                ret(i) = globalID
            }
        }
        return ret
    }
}
