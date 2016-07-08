package test.scala

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.apache.lucene.document.{TextField, Field, Document}
import org.apache.lucene.index.{IndexWriterConfig, DirectoryReader, IndexWriter}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.{ScoreDoc, IndexSearcher}
import org.apache.lucene.store.RAMDirectory
import rhinodog.Core.BlocksWriter
import rhinodog.Core.Definitions.BaseTraits.ITermIterator
import rhinodog.Core.Definitions.DocPosting
import rhinodog.Core.Iterators.{IteratorOR, IteratorAND, MetaIteratorRAM, BlocksIterator}
import rhinodog.Core.MeasureFormats.{OkapiBM25Measure, OkapiBM25MeasureSerializer}

import scala.collection.{Seq, mutable}
import scala.util.Random
import org.junit._

class IteratorsAndQueryingTests {

    @Test
    def benchmark(): Unit = {

        for (i <- 1 to nTestsWithDifferentData) {
            GenerateDataSet()

            //runAllChecksForIterators()
            lastBatch_accMy1 = accMy1
            lastBatch_accMy2 = accMy2
            for(_ <- 1 to nTestsWithSameData) {
                val start = System.nanoTime()
                val res1 = testQueryExecution(K)
                val time1 = System.nanoTime() - start
                //println("merging 1 took " + time1)
                accMy1 += time1
                val start1 = System.nanoTime()
                val res2 = testQueryExecutionAdvanced(K)
                val time2 = System.nanoTime() - start1
                accMy2 += time2
                assert(res1.toArray.sortBy(_._1).sameElements(res2.toArray.sortBy(_._1)))
                //println()
            }
            lastBatch_accMy1 = accMy1 - lastBatch_accMy1
            lastBatch_accMy2 = accMy2 - lastBatch_accMy2
            println("========================")
            print("method1 to method2 ratio ")
            println((lastBatch_accMy1 + 0f)/ lastBatch_accMy2)
            if(runLucene) {
                TestLucene
                print("method 1 to Lucene ratio ")
                println((lastBatch_accMy1 + 0f) / lastBatch_accLucene)
                print("method 2 to Lucene ratio ")
                println((lastBatch_accMy2 + 0f) / lastBatch_accLucene)
            }
        }
        println("tests finnished")
    }

    @Test
    def runAllChecksForIterators() = {
        GenerateDataSet()
        var blockIterator1, blockIterator2, blockIterator3: BlocksIterator = null
        def initBlockIterators: Unit = {
            blockIterator1 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager1.blocks),
                measureSerializer,
                debugList1.length,
                N*10) // *10 - to avoid 0 value of IDF
            blockIterator2 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager2.blocks),
                measureSerializer,
                debugList2.length,
                N*10) // *10 - to avoid 0 value of IDF
            blockIterator3 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager3.blocks),
                measureSerializer,
                debugList3.length,
                N*10) // *10 - to avoid 0 value of IDF
        }
        initBlockIterators
        blocksIteratorTest(blockIterator1, debugList1)
        blocksIteratorTest(blockIterator2, debugList2)
        blocksIteratorTest(blockIterator3, debugList3)

        //AND ITERATOR TEST
        initBlockIterators
        val iteratorAND = new IteratorAND(Array(blockIterator1, blockIterator2, blockIterator3))
        AND_IteratorTest(iteratorAND)

        //AND ITERATOR RANDOM ADVANCE TEST
        initBlockIterators
        val iteratorAND2 = new IteratorAND(Array(blockIterator1, blockIterator2, blockIterator3))
        iteratorRandomAdvanceDocIDTest(iteratorAND2)

        //AND ITERATOR RANDOM ADVANCE to SCORE TEST
        initBlockIterators
        val iteratorAND3 = new IteratorAND(Array(blockIterator1, blockIterator2, blockIterator3))
        val copyAND3 = new IteratorAND(Array(blockIterator1, blockIterator2, blockIterator3))
        iteratorRandomAdvanceScoreTest(iteratorAND3, copyAND3)

        //OR ITERATOR TEST
        initBlockIterators
        val iteratorOR = new IteratorOR(Array(blockIterator1, blockIterator2, blockIterator3))
        OrIteratorTest(iteratorOR)

        //OR ITERATOR RANDOM ADVANCE TEST
        initBlockIterators
        val iteratorOR2 = new IteratorOR(Array(blockIterator1, blockIterator2, blockIterator3))
        iteratorRandomAdvanceDocIDTest(iteratorOR2)

        //OR ITERATOR RANDOM ADVANCE to SCORE TEST
        initBlockIterators
        val iteratorOR3 = new IteratorOR(Array(blockIterator1, blockIterator2, blockIterator3))
        val copyOR3 = new IteratorOR(Array(blockIterator1, blockIterator2, blockIterator3))
        iteratorRandomAdvanceScoreTest(iteratorOR3, copyOR3)

        //2nd level of composition tests
        initBlockIterators
        val iteratorANDAND = new IteratorAND(Array(blockIterator1,
            new IteratorAND(Array(blockIterator2, blockIterator3))))
        AND_IteratorTest(iteratorANDAND)

        initBlockIterators
        val iteratorANDAND2 = new IteratorAND(Array(blockIterator1,
            new IteratorAND(Array(blockIterator2, blockIterator3))))
        iteratorRandomAdvanceDocIDTest(iteratorANDAND2)

        initBlockIterators
        val iteratorANDAND3 = new IteratorAND(Array(blockIterator1,
            new IteratorAND(Array(blockIterator2, blockIterator3))))
        val copyANDAND3 = new IteratorAND(Array(blockIterator1,
            new IteratorAND(Array(blockIterator2, blockIterator3))))
        iteratorRandomAdvanceScoreTest(iteratorANDAND3, copyANDAND3)

        //OR ITERATOR TEST
        initBlockIterators
        val iteratorOROR = new IteratorOR(Array(blockIterator1,
            new IteratorOR(Array(blockIterator2, blockIterator3))))
        OrIteratorTest(iteratorOROR)

        //OR ITERATOR RANDOM ADVANCE TEST
        initBlockIterators
        val iteratorOROR2 = new IteratorOR(Array(blockIterator1,
            new IteratorOR(Array(blockIterator2, blockIterator3))))
        iteratorRandomAdvanceDocIDTest(iteratorOROR2)

        initBlockIterators
        val iteratorOROR3 = new IteratorOR(Array(blockIterator1,
            new IteratorOR(Array(blockIterator2, blockIterator3))))
        val copyOROR3 = new IteratorOR(Array(blockIterator1,
            new IteratorOR(Array(blockIterator2, blockIterator3))))
        iteratorRandomAdvanceScoreTest(iteratorOROR3, copyOROR3)
    }


    val runLucene = true
    val N = 1000 * 10

    val nTestsWithDifferentData = 10
    val nTestsWithSameData = 10

    var accMy1 = 0l
    var accMy2 = 0l
    var accLucene = 0l

    var lastBatch_accMy1 = 0l
    var lastBatch_accMy2 = 0l
    var lastBatch_accLucene = 0l

    val floatMinMeaningfulValue = 0.000001f

    def testQueryExecution(K: Int): mutable.PriorityQueue[(Float, Long)] = {
        val blockIterator1 = new BlocksIterator(
            new MetaIteratorRAM(blocksManager1.blocks),
            measureSerializer,
            debugList1.length,
            N*10, true) // *10 - to avoid 0 value of IDF
        val blockIterator2 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager2.blocks),
                measureSerializer,
                debugList2.length,
                N*10, true) // *10 - to avoid 0 value of IDF
        val blockIterator3 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager3.blocks),
                measureSerializer,
                debugList3.length,
                N*10, true) // *10 - to avoid 0 value of IDF

        val topLevelIterator = new IteratorAND(Array(blockIterator1, blockIterator2, blockIterator3))

        val topK = new mutable.PriorityQueue[(Float, Long)]()(Ordering.by(_._1 * -1))
        var topKPopulated = false
        while (topLevelIterator.hasNext) {
            if (topK.size < K || topLevelIterator.currentScore > topK.head._1) {
                topK += ((topLevelIterator.currentScore, topLevelIterator.currentDocID))
                if (topK.size > K) {
                    topK.dequeue()
                    topKPopulated = true
                }
            }
            topLevelIterator.next()
        }
        topK
    }

    def testQueryExecutionAdvanced(K: Int): mutable.PriorityQueue[(Float, Long)] = {

        val blockIterator1 = new BlocksIterator(new MetaIteratorRAM(blocksManager1.blocks),
            measureSerializer,
            debugList1.length,
            N*10, true) // *10 - to avoid 0 value of IDF
        val blockIterator2 = new BlocksIterator(new MetaIteratorRAM(blocksManager2.blocks),
                measureSerializer,
                debugList2.length,
                N*10, true) // *10 - to avoid 0 value of IDF
        val blockIterator3 = new BlocksIterator(new MetaIteratorRAM(blocksManager3.blocks),
                measureSerializer,
                debugList3.length,
                N*10, true) // *10 - to avoid 0 value of IDF

        val topLevelIterator = new IteratorAND(Array(blockIterator1, blockIterator2, blockIterator3))

        val topK = new mutable.PriorityQueue[(Float, Long)]()(Ordering.by(_._1 * -1))
        var topKPopulated = false
        while (topLevelIterator.hasNext) {
            if (topK.size < K || topLevelIterator.currentScore > topK.head._1) {
                topK += ((topLevelIterator.currentScore, topLevelIterator.currentDocID))
                if (topK.size > K) {
                    topK.dequeue()
                    topKPopulated = true
                }
            }
            topLevelIterator.next()
            if (topKPopulated)
                topLevelIterator.advanceToScore(topK.head._1 + floatMinMeaningfulValue)

        }
        topK
    }

    val random = new Random(System.nanoTime())
    val K = 10
    val norm = 1000

    //For testing - make sure the thirdTermParam % secondTermParam == 0
    // and that MatchEvery == NonMatchEvery

    val secondTermMatchEvery = 8
    val secondTermNonMatchEvery = 2
    val thirdTermMatchEvery = 16
    val thirdTermNonMatchEvery = 64
    //    val secondTermMatchEvery = 256
    //    val secondTermNonMatchEvery = 64
    //    val thirdTermMatchEvery = 2048
    //    val thirdTermNonMatchEvery = 512

    val numberOfTerms = 3

    val measureSerializer = new OkapiBM25MeasureSerializer

    var debugList1: mutable.ArrayBuffer[DocPosting] = null
    var debugList2: mutable.ArrayBuffer[DocPosting] = null
    var debugList3: mutable.ArrayBuffer[DocPosting] = null

    var blocksManager1: BlocksWriter = null
    var blocksManager2: BlocksWriter = null
    var blocksManager3: BlocksWriter = null

    val analyzer = new StandardAnalyzer()

    var directory: RAMDirectory = null
    //new RAMDirectory()
    var iwriter: IndexWriter = null //new IndexWriter(directory, luceneConfig)

    def TestLucene = {
        val ireader = DirectoryReader.open(directory)
        val isearcher = new IndexSearcher(ireader)
        val parser = new QueryParser("fieldname", analyzer)
        val query = parser.parse("term1 term2 term3")
        var hits: Array[ScoreDoc] = null
        lastBatch_accLucene = accLucene
        for(_ <- 1 to nTestsWithSameData) {
            val startLucene = System.nanoTime()
            hits = isearcher.search(query, K).scoreDocs
            val timeLucene = System.nanoTime() - startLucene
            //println("time lucene " + timeLucene)
            accLucene += timeLucene
        }
        lastBatch_accLucene = accLucene - lastBatch_accLucene
        val docs = hits.map(x => (x, isearcher.doc(x.doc)))
        ireader.close()
        directory.close()
    }

    val terms = Array(" term1 ", " term2 ", " term3 ")
    val generateText = (norm: Int, terms: List[(String, Int)]) => {
        val numWords = norm - terms.map(_._2).sum
        val text = " bla " * numWords + terms.map(x => x._1 * x._2).mkString
        text
    }

    def GenerateDataSet(): Unit = {
        debugList1 = new mutable.ArrayBuffer[DocPosting]()
        debugList2 = new mutable.ArrayBuffer[DocPosting]()
        debugList3 = new mutable.ArrayBuffer[DocPosting]()

        directory = new RAMDirectory()
        val luceneConfig = new IndexWriterConfig(analyzer)
        iwriter = new IndexWriter(directory, luceneConfig)

        blocksManager1 = new BlocksWriter(measureSerializer)
        blocksManager2 = new BlocksWriter(measureSerializer)
        blocksManager3 = new BlocksWriter(measureSerializer)

        var prev = 0l
        (0 until N).foreach(i => {
            //val doc = new Document()

            val docID = prev + 1 + /*Short.MaxValue*/ random.nextInt(Short.MaxValue)
            prev = docID
            val tmp = DocPosting(prev,
                OkapiBM25Measure(
                    (random.nextInt(Byte.MaxValue) % 32 + (i /512) % 96).asInstanceOf[Byte]
                    , norm))
            blocksManager1.add(tmp)
            debugList1 += tmp

            if (i % secondTermMatchEvery == 0) {
                val tmp2 = DocPosting(docID,
                    OkapiBM25Measure(
                        (random.nextInt(Byte.MaxValue) % 32 + (i /512) % 96).asInstanceOf[Byte]
                        , norm))
                blocksManager2.add(tmp2)
                debugList2 += tmp2

                if (i % thirdTermMatchEvery == 0) {
                    val tmp3 = DocPosting(docID,
                        OkapiBM25Measure(
                            (random.nextInt(Byte.MaxValue) % 32 + (i /512) % 96).asInstanceOf[Byte]
                            , norm))
                    blocksManager3.add(tmp3)
                    debugList3 += tmp3
                    if(runLucene) {
                        val doc = new Document()
                        val text = generateText(norm, List((terms(2), tmp3.measure.asInstanceOf[OkapiBM25Measure].frequency + 0),
                            (terms(1), tmp2.measure.asInstanceOf[OkapiBM25Measure].frequency + 0),
                            (terms(0), tmp.measure.asInstanceOf[OkapiBM25Measure].frequency + 0)))
                        doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                        doc.add(new Field("ID", docID.toString, TextField.TYPE_STORED))
                        iwriter.addDocument(doc)
                    }

                } else {
                    if(runLucene) {
                        val doc = new Document()
                        val text = generateText(norm,
                            List((terms(1), tmp2.measure.asInstanceOf[OkapiBM25Measure].frequency + 0),
                                (terms(0), tmp.measure.asInstanceOf[OkapiBM25Measure].frequency + 0)))
                        doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                        doc.add(new Field("ID", docID.toString, TextField.TYPE_STORED))
                        iwriter.addDocument(doc)
                    }
                    if (i % thirdTermNonMatchEvery == 0) {
                        val docID3 = prev + 1 + random.nextInt(Byte.MaxValue)
                        prev = docID3
                        val tmp31 = DocPosting(docID3,
                            OkapiBM25Measure(
                                (random.nextInt(Byte.MaxValue) % 32 + (i /512) % 96).asInstanceOf[Byte]
                                , norm))
                        blocksManager3.add(tmp31)
                        debugList3 += tmp31
                        if(runLucene) {
                            val doc = new Document()
                            val text = generateText(norm, List((terms(2), tmp31.measure.asInstanceOf[OkapiBM25Measure].frequency + 0)))
                            doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                            doc.add(new Field("ID", docID3.toString, TextField.TYPE_STORED))
                            iwriter.addDocument(doc)
                        }
                    }
                }
            } else {
                if(runLucene) {
                    val doc = new Document()
                    val text = generateText(norm, List((terms(0), tmp.measure.asInstanceOf[OkapiBM25Measure].frequency + 0)))
                    doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                    doc.add(new Field("ID", docID.toString, TextField.TYPE_STORED))
                    iwriter.addDocument(doc)
                }
                if (i % secondTermNonMatchEvery == 0) {
                    val docID2 = prev + 1 + random.nextInt(Byte.MaxValue)
                    prev = docID2
                    val tmp21 = DocPosting(docID2,
                        OkapiBM25Measure(
                            (random.nextInt(Byte.MaxValue) % 32 + (i /512) % 96).asInstanceOf[Byte]
                            , norm))
                    blocksManager2.add(tmp21)
                    debugList2 += tmp21
                    if(runLucene) {
                        val doc = new Document()
                        val text = generateText(norm, List((terms(1), tmp21.measure.asInstanceOf[OkapiBM25Measure].frequency + 0)))
                        doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                        doc.add(new Field("ID", docID2.toString, TextField.TYPE_STORED))
                        iwriter.addDocument(doc)
                    }
                }
                if (i % thirdTermNonMatchEvery == 0) {
                    val docID3 = prev + 1 + random.nextInt(Byte.MaxValue)
                    prev = docID3
                    val tmp31 = DocPosting(docID3,
                        OkapiBM25Measure(
                            (random.nextInt(Byte.MaxValue) % 32 + (i /512) % 96).asInstanceOf[Byte]
                            , norm))
                    blocksManager3.add(tmp31)
                    debugList3 += tmp31
                    if(runLucene) {
                        val doc = new Document()
                        val text = generateText(norm, List((terms(2), tmp31.measure.asInstanceOf[OkapiBM25Measure].frequency + 0)))
                        doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                        doc.add(new Field("ID", docID3.toString, TextField.TYPE_STORED))
                        iwriter.addDocument(doc)
                    }
                }
            }
        })

        iwriter.close()

        blocksManager1.flushSegment()
        blocksManager1.flushBlock()

        blocksManager2.flushSegment()
        blocksManager2.flushBlock()

        blocksManager3.flushSegment()
        blocksManager3.flushBlock()
    }

    def blocksIteratorTest(iterator: BlocksIterator,
                           debugList: mutable.ArrayBuffer[DocPosting]): Unit = {
        var nonMatched = 0
        for (el <- debugList) {
            val a = iterator.currentDocID
            val b = iterator.currentScore
            if (el.docID != a || el.measure.score != b)
                nonMatched += 1
            iterator.next()
        }
        assert(nonMatched == 0)
    }

    def AND_IteratorTest(iteratorAND: IteratorAND) = {
        var nonMatched = 0
        val matches = (debugList1 ++ debugList2 ++ debugList3)
            .groupBy(_.docID)
            .filter(_._2.length == numberOfTerms)
            .toArray
            .map(x => (x._1, x._2.map(_.measure.score).sum))
            .sortBy(_._1)

        for (i <- matches.indices) {
            val expected = matches(i)
            if (iteratorAND.currentDocID != expected._1 ||
                math.abs(expected._2 - iteratorAND.currentScore) > floatMinMeaningfulValue)
                iteratorAND.next()
        }
        assert(nonMatched == 0)
    }

    def OrIteratorTest(iteratorOR: IteratorOR): Unit = {
        var i = 0
        var nonMatched = 0
        val distinctDocs = (debugList1 ++ debugList2 ++ debugList3).groupBy(_.docID)
            .toArray.map(x => (x._1, x._2.map(_.measure.score).sum)).sortBy(_._1)
        while (i < distinctDocs.length) {
            val expectedDocID = distinctDocs(i)._1
            val expectedScore = distinctDocs(i)._2
            val docID = iteratorOR.currentDocID
            val score = iteratorOR.currentScore
            if (expectedDocID != docID || math.abs(expectedScore - score) > floatMinMeaningfulValue)
                nonMatched += 1
            iteratorOR.next()
            i += 1
        }
        assert(nonMatched == 0)
    }

    def iteratorRandomAdvanceDocIDTest(iterator: ITermIterator) = {
        var i = 0
        val matchesAND = (debugList1 ++ debugList2 ++ debugList3)
            .groupBy(_.docID)
            .filter(_._2.length == numberOfTerms)
            .toArray
            .map(x => (x._1, x._2.map(_.measure.score).sum))
            .sortBy(_._1)

        var nonMatchesOR2 = 0
        while (i < matchesAND.length) {
            val expectedMatch = matchesAND(i)
            iterator.advance(expectedMatch._1)
            if (expectedMatch._1 != iterator.currentDocID ||
                math.abs(expectedMatch._2 - iterator.currentScore) > 0.000001) {
                nonMatchesOR2 += 1
            }
            i += (1 + random.nextInt(10))
        }
        assert(nonMatchesOR2 == 0)
    }

    def randomAdvanceTestHelper(iterator: ITermIterator,
                                matches: Seq[(Long, Float)]): Unit = {
        var nonMatchesOR2 = 0
        var expectedDocID = matches(0)._1
        var targetScore = matches(0)._2
        iterator.next()

        var prevMatchDocID = expectedDocID
        var prevMatchScore = -1f

        var testSubset =matches.dropWhile(_._1 <= prevMatchDocID).take(100).map(x => {
            val rounded = (math.floor(x._2 * 1000000) / 1000000f).toFloat
            (x._1, rounded)
        })
        var nextLargest = testSubset.maxBy(_._2)
        targetScore = (nextLargest._2 - floatMinMeaningfulValue)
        expectedDocID = nextLargest._1

        while (iterator.hasNext && testSubset.nonEmpty) {

            iterator.advanceToScore(targetScore)
            val docID = iterator.currentDocID
            val score = iterator.currentScore

            val d1 = matches.indexWhere(_._1 == docID)
            val d2 = matches.indexWhere(_._1 == expectedDocID)
            val d3 = matches.indexWhere(_._1 == prevMatchDocID)

            if (docID == expectedDocID ||
                (docID > expectedDocID && score > targetScore)) {
                prevMatchDocID = docID
                prevMatchScore = score
            } else {
                nonMatchesOR2 += 1
            }
            iterator.next()
            testSubset = matches.dropWhile(_._1 <= prevMatchDocID).take(100).map(x => {
                val rounded = (math.floor(x._2 * 1000000) / 1000000f).toFloat
                (x._1, rounded)
            })
            if(testSubset.nonEmpty) {
                nextLargest = testSubset.maxBy(_._2)
                targetScore = (nextLargest._2  - floatMinMeaningfulValue)
                expectedDocID = nextLargest._1
            }
        }
        assert(nonMatchesOR2 == 0)
    }

    def iteratorRandomAdvanceScoreTest(iterator: IteratorAND, copy: IteratorAND) = {
        val actualMatches = mutable.Buffer[(Long, Float)]()
        while (copy.currentDocID != -1) {
            actualMatches += ((copy.currentDocID, copy.currentScore))
            copy.next()
        }
        randomAdvanceTestHelper(iterator, actualMatches)
    }

    def iteratorRandomAdvanceScoreTest(iterator: IteratorOR, copy: IteratorOR) = {
        val actualMatches = mutable.Buffer[(Long, Float)]()
        while (copy.currentDocID != -1) {
            actualMatches += ((copy.currentDocID, copy.currentScore))
            copy.next()
        }
        randomAdvanceTestHelper(iterator, actualMatches)
    }

}
