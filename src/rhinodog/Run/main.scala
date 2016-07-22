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

import java.nio.file.{Paths, Path}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}

import org.apache.lucene.analysis.en
import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.lucene.document.{TextField, Field}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{ScoreDoc, IndexSearcher}
import org.apache.lucene.store._
import org.slf4j.LoggerFactory
import rhinodog.Analysis.EnglishAnalyzer
import rhinodog.Core.Definitions.Configuration.storageModeEnum
import rhinodog.Core.Definitions.{TermMetadata, Document}
import rhinodog.Core.Iterators.IteratorAND
import rhinodog.Core.{ORClause, ANDClause, TermToken, InvertedIndex}
import rhinodog.Core.MeasureFormats.{BMLikeMeasureSerializer, BMLikeMeasure, OkapiBM25MeasureSerializer, OkapiBM25Measure}
import scala.collection.JavaConverters._

import info.bliki.wiki.dump.IArticleFilter
import info.bliki.wiki.dump.Siteinfo
import info.bliki.wiki.dump.WikiArticle
import info.bliki.wiki.dump.WikiXMLParser
import java.io._
import sun.nio.fs._

import scala.collection._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object main {

    //BMLikeMeasureSerializer
    val invertedIndex = new InvertedIndex(new BMLikeMeasureSerializer(),
        new EnglishAnalyzer(),
        storageModeEnum.READ_WRITE)

    val logger = LoggerFactory.getLogger(this.getClass)

    val directory = new MMapDirectory(Paths.get("Lucene"))
    val stopList = scala.io.Source.fromFile("D:\\Workspace\\scalaMMapTest master\\stopWordsList.txt")
        .getLines().toSeq.asJava
    val charArraySet = new CharArraySet(stopList, true)
    val analyzer = new en.EnglishAnalyzer(charArraySet)
    val luceneConfig = new IndexWriterConfig(analyzer)
    val iwriter = new IndexWriter(directory, luceneConfig)

    def main(args: Array[String]): Unit = {

        val folder = new File("G:\\Wiki")
        logger.info("starting indexing ")
        //val counter = new AtomicLong(0)
        folder.listFiles().par.foreach(file => {
            var text = ""
            try {
                text = scala.io.Source.fromFile(file,"Windows-1252").mkString
            }
            catch { case e: java.nio.charset.MalformedInputException => logger.info("bad file") }
            if(text.nonEmpty) {
                val ID = invertedIndex.addDocument(Document(file.getName, text))
                val doc = new org.apache.lucene.document.Document()
                doc.add(new Field("ID", file.getName, TextField.TYPE_STORED))
                doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                iwriter.addDocument(doc)
                if (ID % 20000 == 0) {
                    iwriter.commit()
                    invertedIndex.flush()
                }
            }
        })
        logger.info("done indexing ")

//        val allMeta = invertedIndex._mainComponents.metadataManager.getAllMetadata
//        val tmp1 = allMeta.sortWith(_._2.numberOfDocs > _._2.numberOfDocs)
//            //.drop(2000*2)
//            .take(300)
//        val mostFrequentTerms1 = tmp1.map(
//            (term) => invertedIndex._mainComponents.repository.getTerm(term._1))
//            .filter(_.forall(_.isLetter))
//        val tmp2 = allMeta.sortWith(_._2.numberOfDocs > _._2.numberOfDocs)
//            .drop(2000)
//            .take(300)
//        val mostFrequentTerms2 = tmp2.map(
//            (term) => invertedIndex._mainComponents.repository.getTerm(term._1))
//            .filter(_.forall(_.isLetter))
//
//        val K = 100//0
//        //        println("press Enter to start benchmark")
//        //        scala.io.StdIn.readLine()
//
//        var resultsRhinodog = new ArrayBuffer[(String, Array[(Float, Long)])]()
//        val start = System.currentTimeMillis()
//        for (i <- 0 until mostFrequentTerms1.length - 1)
//            for (j <- i + 1 until mostFrequentTerms2.length) {
//                val andClause = new ORClause(Array(TermToken(mostFrequentTerms1(i)),
//                    TermToken(mostFrequentTerms2(j))))
//                //val andClause = TermToken(mostFrequentTerms(i))
//                val queryWords = mostFrequentTerms1(i) + " " + mostFrequentTerms2(j)
//                val ret = invertedIndex.getQueryEngine().executeQuery(andClause, K)
////                if("australia agreement" == queryWords)
////                    for(x <- ret) {
////                        val ID = invertedIndex.getDocument(x._2).get.ID
////                        println("=======")
////                        println(ID)
////                        println(x._1)
////                        val explain = invertedIndex.getQueryEngine().explain(andClause,x._2)
////                        println(explain)
////                    }
//                resultsRhinodog += ((queryWords, ret.toArray))
//            }
//        val time = System.currentTimeMillis() - start
//        logger.info("Rhinodog search took {} ms", time)
//
//        val ireader = DirectoryReader.open(directory)
//        val isearcher = new IndexSearcher(ireader)
//        val standardAnalyzer = new StandardAnalyzer(charArraySet)
//        val parser = new QueryParser("fieldname", standardAnalyzer)
//
//        val resultsLucene = new ArrayBuffer[(String, Array[ScoreDoc])]()
//        val start1 = System.currentTimeMillis()
//        for (i <- 0 until mostFrequentTerms1.length - 1)
//            for (j <- i + 1 until mostFrequentTerms2.length) {
//                val queryWords = mostFrequentTerms1(i) + " " + mostFrequentTerms2(j)
//                val query = parser.parse(mostFrequentTerms1(i) + " OR " + mostFrequentTerms2(j))
//                val hits = isearcher.search(query, K).scoreDocs
////                if("australia agreement" == queryWords)
////                for (h <- hits) {
////                    val docID = ireader.document(h.doc)
////                        .getField("ID")
////                        .stringValue()
////                    println(docID)
////                    println(isearcher.explain(query, h.doc))
////                    println("======")
////                }
//                resultsLucene += ((queryWords, hits))
//            }
//        val time1 = System.currentTimeMillis() - start1
//        logger.info("Lucene search took {} ms", time1)
//
//        val resultsLucene2 = resultsLucene.map(x =>
//            x._2.map(y => (y.score,
//                ireader.document(y.doc)
//                    .getField("ID")
//                    .stringValue())))
//
//        val resultsRhinodog2 = resultsRhinodog.map(x =>
//            (x._1, x._2.map(y => (y._1, invertedIndex.getDocument(y._2).get.ID))))
//
//        val test = resultsRhinodog2.zip(resultsLucene2).map(all => {
//            val tmpMy = all._1._2.sortWith(_._1 > _._1)
//            val tmpLucene = all._2.sortWith(_._1 > _._1)
//            val test1 = tmpMy.count(x => tmpLucene.exists(y => y._2 == x._2))
//            val test2 = if (tmpLucene.isEmpty && tmpMy.isEmpty) 1
//            else if (tmpLucene.isEmpty && tmpMy.nonEmpty) 0
//            else (test1 + 0f) / tmpLucene.size
//            var index = 0
//            val test3 = tmpMy.zipWithIndex.map(x => {
//                val correctGuess = tmpLucene.exists(y => y._2 == x._1._2)
//                if(correctGuess) {
//                    index += 1
//                    index / (x._2 + 1f)
//                } else 0
//            }).sum / tmpMy.size
//            (test2, all._1._1, test3, tmpLucene.zip(tmpMy))
//        })
//
//        logger.info("closing")
//        invertedIndex.close()
//        directory.close()

        println()
    }


    private class DemoArticleFilter extends IArticleFilter {
        var counter = 0
        val folder = "G:\\Wiki"

        def process(page: WikiArticle, siteinfo: Siteinfo) {
            val text: String = page.getText
            if (text.length > 100 && !text.startsWith("#REDIRECT")) {
                try {
                    Future {
                        val file = new File(folder + File.separator + counter + ".txt")
                        if(!file.exists()) {
                            file.createNewFile()
                            val fc = new FileOutputStream(file)
                            fc.write(text.getBytes())
                        }
                    }
                } catch {
                    case ex: Exception => ex.printStackTrace()
                }
                if(counter % 1000 == 0)
                    println(s"new docID = $counter ")
                counter += 1
            }
        }
    }

    def run() {
        try {
            val handler: IArticleFilter = new DemoArticleFilter
            val wxp: WikiXMLParser = new WikiXMLParser(new File("G:\\enwiki-20160501-pages-articles.xml.bz2"), handler)
            wxp.parse
        }
        catch {
            case e: Exception => {
                e.printStackTrace
            }
        }
    }
}
