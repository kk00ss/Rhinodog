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
package rhinodog

package object Analysis {

    import rhinodog.Core.Definitions.BaseTraits.IRepository
    import scala.collection._

    case class TokenInfo(ID: Int, var frequency: Int)

    /** counts frequency for every root form of a word  **/
    class LocalLexicon(repository: IRepository) {
        //cache for Repository getTermID and idToRoot in one hop
        val root2TokenInfo: mutable.Map[String, TokenInfo] = mutable.Map()

        private var wordsAdded = 0
        private var maxFreq = 0f

        def maxFrequency = maxFreq

        def WordsAdded = wordsAdded

        // depends on proper pooling implementation
        //        def reset() = {
        //            wordsAdded = 0
        //            maxFreq = 0f
        //            root2TokenInfo.clear()
        //        }

        def addWord(rootForm: String): Int = {
            var ret = -1
            if(rootForm.length > 1) {
                wordsAdded += 1
                val previous = root2TokenInfo.get(rootForm)
                if (previous.isDefined) {
                    val tokenInfo = previous.get
                    ret = tokenInfo.ID
                    tokenInfo.frequency += 1
                    if (tokenInfo.frequency > maxFreq)
                        maxFreq = tokenInfo.frequency
                }
                else {
                    val globalID = repository.getTermID(rootForm)
                    val newLW = new TokenInfo(globalID, 1)
                    root2TokenInfo += (rootForm -> newLW)
                    ret = globalID
                }
            }
            return ret
        }
    }

}
