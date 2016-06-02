package test.scala

import org.junit.Test
import rhinodog.Core.Definitions.DocPosting
import rhinodog.Core.MeasureFormats._
import rhinodog.Core.Utils.DocPostingsSerializer

import scala.util.Random

class DocPostingSerializerTest {
    @Test
    def testDocTermSerializer() = {
        val N = 100
        val data = new Array[DocPosting](N)
        for (i <- 1 to N) {
            data(i - 1) = new DocPosting(i, new MeasureSimple(i.asInstanceOf[Byte], i))
        }
        val serializer = new DocPostingsSerializer(new MeasureSerializerSimple())
        val segmentSerialized = serializer.encodeIntoComponents(data)
        val uncompressed = serializer.decodeFromComponents(segmentSerialized)
        assert(uncompressed.toList == data.toList)
    }

    @Test
    def testSegmentSize() = {
        val N = 400
        val random = new Random(System.nanoTime())
        val data = new Array[DocPosting](N)
        var prevId = 0
        for (i <- 1 to N) {
            var id = prevId + random.nextInt(Short.MaxValue)
            if(id == prevId)
               id += random.nextInt(Short.MaxValue)
            prevId = id
            data(i - 1) = new DocPosting(id,
                new MeasureSimple(math.pow(id % 3, 2).asInstanceOf[Byte], random.nextInt(Short.MaxValue)))
        }
        val serializer = new DocPostingsSerializer(new MeasureSerializerSimple())
        val segmentSerialized = serializer.encodeIntoComponents(data)
        val uncompressed = serializer.decodeFromComponents(segmentSerialized)
        assert(uncompressed.toList == data.toList)
    }
}
