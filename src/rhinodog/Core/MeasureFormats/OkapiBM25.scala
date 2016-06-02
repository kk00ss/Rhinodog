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
package rhinodog.Core.MeasureFormats

import java.nio.ByteBuffer

import rhinodog.Core.Definitions._

object OkapiBM25 {
    val avgTermsPerDoc = 500
    val k = 2
    val b = 0.5
}

case class OkapiBM25(frequency: Byte, norm: Int) extends Measure {
    type Self = OkapiBM25
    def compare(that: OkapiBM25) = this.score.compare(that.score)
    def numberOfBytesRequired = 5
    def score = {
            //TODO: create MeasureFloat
            //TODO: move okapi bm25 computation into analysis and only store float score
        val a = frequency * (1 + OkapiBM25.k)
        val c = frequency + OkapiBM25.k *
            (1 - OkapiBM25.b + OkapiBM25.b * norm / OkapiBM25.avgTermsPerDoc)
        (a / c).asInstanceOf[Float]
    }
    def getSerializer = new OkapiBM25Serializer()
}

class OkapiBM25Serializer extends MeasureSerializerBase {
    def numberOfComponentsRequired = 2
    def numberOfBytesRequired = 5

    def MinValue: Measure = OkapiBM25(0,1)

    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float =
        if(components.length != numberOfComponentsRequired + 1)
            throw new IllegalArgumentException("wrong number of components")
        else components(1)(measurePosition).asInstanceOf[Float]/components(2)(measurePosition)

    def serialize(_m: Measure, buf: ByteBuffer) = {
        val m = _m.asInstanceOf[OkapiBM25]
        buf.put(m.frequency) //because it writes 8 lowest bits
        buf.putInt(m.norm)
    }
    def deserialize(buf: ByteBuffer): Measure = {
        val freq = buf.get()
        val norm = buf.getInt()
        return OkapiBM25(freq,norm)
    }
    //component 0 - is used for doc Ids
    def writeToComponents(_m: Measure, components: Array[Array[Int]], i: Int): Unit = {
        val m = _m.asInstanceOf[OkapiBM25]
        components(1)(i) = m.frequency.asInstanceOf[Int]
        components(2)(i) = m.norm
    }
    def readFromComponents(components: Array[Array[Int]], i: Int): Measure =
        OkapiBM25(components(1)(i).asInstanceOf[Byte], components(2)(i))
}
