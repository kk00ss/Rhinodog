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

import java.util.concurrent.atomic.AtomicInteger

import rhinodog.Analysis.EnglishAnalyzer
import rhinodog.Core.Definitions.Configuration.storageModeEnum
import rhinodog.Core.Definitions.Document
import rhinodog.Core.{TermToken, InvertedIndex}
import rhinodog.Core.MeasureFormats.{OkapiBM25MeasureSerializer, OkapiBM25Measure}

import info.bliki.wiki.dump.IArticleFilter
import info.bliki.wiki.dump.Siteinfo
import info.bliki.wiki.dump.WikiArticle
import info.bliki.wiki.dump.WikiXMLParser
import java.io._


import scala.collection._

object main {

    val invertedIndex = new InvertedIndex(new OkapiBM25MeasureSerializer(),
        new EnglishAnalyzer(),
        storageModeEnum.READ_WRITE)

    def main(args: Array[String]): Unit = {

                val folder = new File("WikiText")
                folder.listFiles().par.foreach(file =>  {
                    val text = scala.io.Source.fromFile(file).mkString
                    val ID = invertedIndex.addDocument(Document(text))
                })

//        val topLevelIterator = invertedIndex
//            .getQueryEngine()
//            .buildTopLevelIterator(TermToken("clock"))
//
//        val ret = invertedIndex.getQueryEngine().executeQuery(topLevelIterator, 10)

        invertedIndex.close()

        println()
    }

    private class DemoArticleFilter extends IArticleFilter {
        var counter = 0
        var totalSize = 0

        def process(page: WikiArticle, siteinfo: Siteinfo) {
            val text: String = page.getText
            val folder = "WikiText"
            if (text.length > 100 && !text.startsWith("#REDIRECT")) {
                totalSize += text.length
                try {
                    val file = new File(folder + File.separator + counter + ".txt")
                    file.createNewFile()
                    val fc = new FileOutputStream(file)
                    fc.write(text.getBytes())
                } catch {
                    case ex: Exception => ex.printStackTrace()
                }
                //                val ID = invertedIndex.addDocument(Document(text))
                println(s"new docID = $counter total documentsSize = $totalSize")
                counter += 1
            }
        }
    }

    def run(bz2Filename: String) {
        try {
            val handler: IArticleFilter = new DemoArticleFilter
            val wxp: WikiXMLParser = new WikiXMLParser(new File(bz2Filename), handler)
            wxp.parse
        }
        catch {
            case e: Exception => {
                e.printStackTrace
            }
        }
    }
}
