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
package rhinodog.Run

import rhinodog.Analysis.EnglishAnalyzer
import rhinodog.Core.Definitions.Configuration.storageModeEnum
import rhinodog.Core.Definitions.Document
import rhinodog.Core.{ElementaryClause, InvertedIndex}
import rhinodog.Core.MeasureFormats.{OkapiBM25MeasureSerializer, OkapiBM25Measure}

import scala.collection._

object main {

    def main(args: Array[String]): Unit = {
        scala.io.StdIn.readLine()
        val invertedIndex = new InvertedIndex(new OkapiBM25MeasureSerializer(),
                                              new EnglishAnalyzer(),
                                              storageModeEnum.CREATE)
        val document = scala.io.Source.fromFile("testArticle.txt").mkString
        var start = System.currentTimeMillis()
        val ID = invertedIndex.addDocument(Document(document))
        var time = System.currentTimeMillis() - start
        println(s"analysis time $time")
        start = System.currentTimeMillis()
        invertedIndex.flush()
        time = System.currentTimeMillis() - start
        println(s"flush time $time")

        val topLevelIterator = invertedIndex
            .getQueryEngine()
            .buildTopLevelIterator(ElementaryClause(1))

        val ret = invertedIndex.getQueryEngine().executeQuery(topLevelIterator, 10)

        println()
    }
}
