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


//TODO: load from settings
object OkapiBM25 {
    val avgTermsPerDoc = 3309 //500
    val k = 1.2f
    val b = 0.75f
    val tmp1 = k + 1
    val tmp2 = 1 - b
    def compute(freq: Int, norm: Int): Float = {
        val a = freq * OkapiBM25.tmp1
        val c = freq + OkapiBM25.k *
            (OkapiBM25.tmp2 + OkapiBM25.b * norm / OkapiBM25.avgTermsPerDoc)
        a / c
    }
}

case class OkapiBM25Measure(frequency: Short, norm: Int) extends Measure {
    if(frequency < 0)
        throw new IllegalArgumentException("frequency cannot be negative or zero")
    type Self = OkapiBM25Measure
    def compare(that: OkapiBM25Measure) = this.score.compare(that.score)
    def score: Float = OkapiBM25.compute(frequency, norm)
    def getSerializer = new OkapiBM25MeasureSerializer()
}

class OkapiBM25MeasureSerializer extends MeasureSerializerBase {
    val numberOfComponentsRequired = 2
    val compressFlags = List(true,true)
    def numberOfBytesRequired = 6

    def MinValue: Measure = OkapiBM25Measure(0,1)

    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float =
        if(components.length != numberOfComponentsRequired + 1)
            throw new IllegalArgumentException("wrong number of components")
        else {
            val frequency = components(1)(measurePosition)
            val norm = components(2)(measurePosition)
            OkapiBM25.compute(frequency, norm)
        }

    def serialize(_m: Measure, buf: ByteBuffer) = {
        val m = _m.asInstanceOf[OkapiBM25Measure]
        buf.putShort(m.frequency) //because it writes 8 lowest bits
        buf.putInt(m.norm)
    }
    def deserialize(buf: ByteBuffer): Measure = {
        val freq = buf.getShort()
        val norm = buf.getInt()
        return OkapiBM25Measure(freq,norm)
    }
    //component 0 - is used for doc Ids
    def writeToComponents(_m: Measure, components: Array[Array[Int]], i: Int): Unit = {
        val m = _m.asInstanceOf[OkapiBM25Measure]
        components(1)(i) = m.frequency.asInstanceOf[Int]
        components(2)(i) = m.norm
    }
    def readFromComponents(components: Array[Array[Int]], i: Int): Measure =
        OkapiBM25Measure(components(1)(i).asInstanceOf[Short], components(2)(i))
}
