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
            data(i - 1) = new DocPosting(i, new OkapiBM25Measure(i.asInstanceOf[Byte], i))
        }
        val serializer = new DocPostingsSerializer(new OkapiBM25MeasureSerializer())
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
                new OkapiBM25Measure(math.pow(id % 3, 2).asInstanceOf[Byte], random.nextInt(Short.MaxValue)))
        }
        val serializer = new DocPostingsSerializer(new OkapiBM25MeasureSerializer())
        val segmentSerialized = serializer.encodeIntoComponents(data)
        val uncompressed = serializer.decodeFromComponents(segmentSerialized)
        assert(uncompressed.toList == data.toList)
    }
}
