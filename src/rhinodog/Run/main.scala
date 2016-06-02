package rhinodog.Run

import java.io.StringReader
import java.nio.ByteBuffer

import edu.emory.mathcs.nlp.tokenization.EnglishTokenizer

import org.apache.lucene.analysis.standard.{StandardAnalyzer, StandardTokenizer}
import org.apache.lucene.document.{Document, Field, TextField}
import org.apache.lucene.index.{DirectoryReader, IndexWriter, IndexWriterConfig}
import org.apache.lucene.queryparser.classic.QueryParser
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.RAMDirectory

import rhinodog.Analysis.AnalyzerForMeasureSimple
import rhinodog.Core.BlocksWriter
import rhinodog.Core.Definitions.BaseTraits._
import rhinodog.Core.Definitions._
import rhinodog.Core.Iterators._
import rhinodog.Core.Utils.DocPostingsSerializer

import rhinodog.Core.MeasureFormats.{MeasureSerializerSimple, MeasureSimple}

import scala.collection._
import scala.util.Random

object main {

    def main(args: Array[String]): Unit = {
        //val t = new QueryEngineTest()
        //t.test2
        val inputFile = "testArticle.txt"
        val text = scala.io.Source.fromFile(inputFile).mkString

        scala.io.StdIn.readLine()
        val tokenizer = new EnglishTokenizer()

        (1 to 200).foreach(i => {
            val start = System.currentTimeMillis()
            //testStandardAnalyzer(text)
            (new AnalyzerForMeasureSimple(null)).analyze(text)
            println("analysis took " + (System.currentTimeMillis() - start))
        })

        GenerateDataSet()

        //test1

        runAllChecksForIterators()
        for (i <- 1 to 100) {
            GenerateDataSet()
            val start = System.nanoTime()
            testQueryExecution(K)
            val time = System.nanoTime() - start
            println("merging took " + time)
            TestLucene
            System.gc()
        }
    }

    val source = new StandardTokenizer()

    def testStandardAnalyzer(text: String): Seq[String] = {
        source.setReader(new StringReader(text))
        source.reset()
        val tokens = mutable.Buffer[String]()
        while (source.incrementToken())
            tokens += source.getAttributeImplsIterator.next().toString
        source.close()
        tokens
    }


    def test1 = {
        val numComponents = measureSerializer.numberOfComponentsRequired + 1

        val segmentReadBuffer = new SegmentSerialized(numComponents)
        val encodedLengths = new Array[Int](numComponents)
        val segmentDecodedBuffer = new SegmentSerialized(numComponents)
        var maxDocIDsOffset = 0

        segmentReadBuffer(0) = new Array[Int](DocPostingsSerializer.segmentSize * 3 + 1)
        segmentDecodedBuffer(0) = new Array[Int](DocPostingsSerializer.segmentSize * 3 + 1)
        for (i <- 1 to measureSerializer.numberOfComponentsRequired) {
            segmentReadBuffer(i) = new Array[Int](DocPostingsSerializer.segmentSize)
            segmentDecodedBuffer(i) = new Array[Int](DocPostingsSerializer.segmentSize)
        }

        val buffer = ByteBuffer.wrap(blocksManager1.blocks.head.data)

        val segmentIterator = new SerializedSegmentIterator(segmentDecodedBuffer,
            measureSerializer,
            maxDocIDsOffset)

        for (i <- 0 to 4) {

            buffer.position(buffer.position + 15)
            val dataStartPosition = buffer.position()

            val data = DocPostingsSerializer.readComponents(buffer)
            val uncompressed = DocPostingsSerializer.decodeComponents(data)

            buffer.position(dataStartPosition)

            DocPostingsSerializer.readIntoBuffer(buffer, segmentReadBuffer, encodedLengths)
            maxDocIDsOffset = DocPostingsSerializer.decodeIntoBuffer(segmentReadBuffer,
                segmentDecodedBuffer,
                encodedLengths)

            segmentIterator.maxDocIDOffset = maxDocIDsOffset
            segmentIterator.reset()

            while (segmentIterator.currentDocID != -1) {
                println(segmentIterator.currentDocID)
                segmentIterator.next()
            }

            println()
            //            segmentReadBuffer.foreach(_.foreach( _ = 0 ))
            //            segmentDecodedBuffer.foreach(_.foreach( _ = 0 ))
            encodedLengths.indices.foreach(i => encodedLengths(i) = 0)
        }
    }

    val floatMinMeaningfulValue = 0.000001f

    def testQueryExecution(K: Int): Unit = {
        val blockIterator1 = new BlocksIterator(new MetaIteratorRAM(blocksManager1.blocks),
                                                measureSerializer,
                                                blocksManager1.prevDocID,
                                                N)
        val blockIterator2 = new BlocksIterator(new MetaIteratorRAM(blocksManager2.blocks),
                                                measureSerializer,
                                                blocksManager2.prevDocID,
                                                N)
        val blockIterator3 = new BlocksIterator(new MetaIteratorRAM(blocksManager3.blocks),
                                                measureSerializer,
                                                blocksManager3.prevDocID,
                                                N)

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
    }

    val random = new Random(System.nanoTime())
    val K = 10
    val N = 1000 * 10
    val norm = 1000

    //For testing - make sure the thirdTermParam % secondTermParam == 0
    // and that MatchEvery == NonMatchEvery
    val secondTermMatchEvery = 33
    val secondTermNonMatchEvery = 3
    val thirdTermMatchEvery = 66
    val thirdTermNonMatchEvery = 11
    val numberOfTerms = 3

    val measureSerializer = new MeasureSerializerSimple

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
        val startLucene = System.nanoTime()
        val hits = isearcher.search(query, K).scoreDocs
        val timeLucene = System.nanoTime() - startLucene
        println("time lucene " + timeLucene)
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
                MeasureSimple(random.nextInt(Byte.MaxValue).asInstanceOf[Byte], norm))
            blocksManager1.add(tmp)
            debugList1 += tmp

            if (i % secondTermMatchEvery == 0) {
                val tmp2 = DocPosting(docID,
                    MeasureSimple(random.nextInt(Byte.MaxValue).asInstanceOf[Byte], norm))
                blocksManager2.add(tmp2)
                debugList2 += tmp2

                if (i % thirdTermMatchEvery == 0) {
                    val tmp3 = DocPosting(docID,
                        MeasureSimple(random.nextInt(Byte.MaxValue).asInstanceOf[Byte], norm))
                    blocksManager3.add(tmp3)
                    debugList3 += tmp3

                    val doc = new Document()
                    val text = generateText(norm, List((terms(2), tmp3.measure.asInstanceOf[MeasureSimple].frequency + 0),
                        (terms(1), tmp2.measure.asInstanceOf[MeasureSimple].frequency + 0),
                        (terms(0), tmp.measure.asInstanceOf[MeasureSimple].frequency + 0)))
                    doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                    doc.add(new Field("ID", docID.toString, TextField.TYPE_STORED))
                    iwriter.addDocument(doc)

                } else {
                    val doc = new Document()
                    val text = generateText(norm,
                        List((terms(1), tmp2.measure.asInstanceOf[MeasureSimple].frequency + 0),
                            (terms(0), tmp.measure.asInstanceOf[MeasureSimple].frequency + 0)))
                    doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                    doc.add(new Field("ID", docID.toString, TextField.TYPE_STORED))
                    iwriter.addDocument(doc)

                    if (i % thirdTermNonMatchEvery == 0) {
                        val docID3 = prev + 1 + random.nextInt(Byte.MaxValue)
                        prev = docID3
                        val tmp31 = DocPosting(docID3,
                            MeasureSimple(random.nextInt(Byte.MaxValue).asInstanceOf[Byte], norm))
                        blocksManager3.add(tmp31)
                        debugList3 += tmp31

                        val doc = new Document()
                        val text = generateText(norm, List((terms(2), tmp31.measure.asInstanceOf[MeasureSimple].frequency + 0)))
                        doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                        doc.add(new Field("ID", docID3.toString, TextField.TYPE_STORED))
                        iwriter.addDocument(doc)
                    }
                }
            } else {
                val doc = new Document()
                val text = generateText(norm, List((terms(0), tmp.measure.asInstanceOf[MeasureSimple].frequency + 0)))
                doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                doc.add(new Field("ID", docID.toString, TextField.TYPE_STORED))
                iwriter.addDocument(doc)

                if (i % secondTermNonMatchEvery == 0) {
                    val docID2 = prev + 1 + random.nextInt(Byte.MaxValue)
                    prev = docID2
                    val tmp21 = DocPosting(docID2,
                        MeasureSimple(random.nextInt(Byte.MaxValue).asInstanceOf[Byte], norm))
                    blocksManager2.add(tmp21)
                    debugList2 += tmp21

                    val doc = new Document()
                    val text = generateText(norm, List((terms(1), tmp21.measure.asInstanceOf[MeasureSimple].frequency + 0)))
                    doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                    doc.add(new Field("ID", docID2.toString, TextField.TYPE_STORED))
                    iwriter.addDocument(doc)
                }
                if (i % thirdTermNonMatchEvery == 0) {
                    val docID3 = prev + 1 + random.nextInt(Byte.MaxValue)
                    prev = docID3
                    val tmp31 = DocPosting(docID3,
                        MeasureSimple(random.nextInt(Byte.MaxValue).asInstanceOf[Byte], norm))
                    blocksManager3.add(tmp31)
                    debugList3 += tmp31

                    val doc = new Document()
                    val text = generateText(norm, List((terms(2), tmp31.measure.asInstanceOf[MeasureSimple].frequency + 0)))
                    doc.add(new Field("fieldname", text, TextField.TYPE_STORED))
                    doc.add(new Field("ID", docID3.toString, TextField.TYPE_STORED))
                    iwriter.addDocument(doc)
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
        var matches = 0
        for (el <- debugList) {
            val a = iterator.currentDocID
            val b = iterator.currentScore
            if (el.docID == a && el.measure.score == b)
                matches += 1
            iterator.next()
        }
        assert(matches == debugList.length)
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
                math.abs(expected._2 - iteratorAND.currentScore) > 0.000001)
                nonMatched += 1
            //            if(i >= 66)
            //                print()
            iteratorAND.next()
        }
        //val expectedMatches = 1 + debugList1.length / thirdTermParam
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
            if (expectedDocID != docID || math.abs(expectedScore - score) > 0.000001)
                nonMatched += 1
            iteratorOR.next()
            i += 1
        }
        assert(nonMatched == 0)
    }

    def iteratorRandomAdvanceDocIDTest(iterator: TermIteratorBase) = {
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

    def randomAdvanceTestHelper(iterator: TermIteratorBase,
                                matches: Array[(Long, Float)]): Unit = {
        var nonMatchesOR2 = 0
        var expectedDocID = matches(0)._1
        var targetScore = matches(0)._2
        iterator.next()

        var prevMatchDocID = expectedDocID
        var prevMatchScore = -1f

        var testSubset = matches.dropWhile(_._1 <= expectedDocID).take(100)

        while (iterator.hasNext && testSubset.nonEmpty) {
            val nextLargest = testSubset.maxBy(_._2)
            targetScore = (math.floor(nextLargest._2 * 1000000) / 1000000 - 0.00001).asInstanceOf[Float]
            expectedDocID = nextLargest._1

            iterator.advanceToScore(targetScore)
            val docID = iterator.currentDocID
            val score = iterator.currentScore

            val d1 = matches.indexWhere(_._1 == docID)
            val d2 = matches.indexWhere(_._1 == expectedDocID)
            val d3 = matches.indexWhere(_._1 == prevMatchDocID)

            if (math.abs(score - nextLargest._2) < 0.000001 && docID == expectedDocID) {
                prevMatchDocID = docID
                prevMatchScore = score
            } else {
                val tmp = matches.slice(d3 + 1, math.max(d1, d2) + 1)
                if (tmp.nonEmpty) {
                    val largest = tmp.maxBy(_._2)
                    if (largest._1 != docID || math.abs(score - largest._2) > 0.000001) {
                        nonMatchesOR2 += 1
                    } else {
                        prevMatchDocID = docID
                        prevMatchScore = score
                    }
                }
            }
            iterator.next()
            testSubset = matches.dropWhile(_._1 <= expectedDocID).take(100)
        }
        assert(nonMatchesOR2 == 0)
    }

    def iteratorRandomAdvanceScoreTest(iterator: IteratorAND) = {
        val matches = (debugList1 ++ debugList2 ++ debugList3)
            .groupBy(_.docID)
            .filter(_._2.length == numberOfTerms)
            .toArray
            .map(x => (x._1, x._2.map(_.measure.score).sum))
            .sortBy(_._1)
        randomAdvanceTestHelper(iterator, matches)
    }

    def iteratorRandomAdvanceScoreTest(iterator: IteratorOR) = {
        val matches = (debugList1 ++ debugList2 ++ debugList3)
            .groupBy(_.docID)
            .toArray
            .sortBy(_._1)
            .map(x => (x._1, x._2.map(_.score).sum))
        randomAdvanceTestHelper(iterator, matches)
    }

    def runAllChecksForIterators() = {

        var blockIterator1, blockIterator2, blockIterator3: BlocksIterator = null
        def initBlockIterators: Unit = {
            blockIterator1 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager1.blocks),
                measureSerializer,
                blocksManager1.prevDocID,
                N)
            blockIterator2 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager2.blocks),
                measureSerializer,
                blocksManager2.prevDocID,
                N)
            blockIterator3 = new BlocksIterator(
                new MetaIteratorRAM(blocksManager3.blocks),
                measureSerializer,
                blocksManager3.prevDocID,
                N)
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
        iteratorRandomAdvanceScoreTest(iteratorAND3)

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
        iteratorRandomAdvanceScoreTest(iteratorOR3)

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
        iteratorRandomAdvanceScoreTest(iteratorANDAND3)

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
        iteratorRandomAdvanceScoreTest(iteratorOROR3)
    }

}

