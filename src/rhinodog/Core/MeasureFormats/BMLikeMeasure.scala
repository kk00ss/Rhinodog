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
object BMLikeMeasure {
    //TODO: add boosting
    def compute(freq: Int, docLength: Int): Float = {
       //def smoothing = (x:Double) => x/(x+1)
       val res = freq*2.2/(freq + math.sqrt(docLength)/100)
        res.toFloat
    }
}

case class BMLikeMeasure(frequency: Short, docLength: Int) extends Measure {
    if(frequency < 0)
        throw new IllegalArgumentException("frequency cannot be negative or zero")
    type Self = BMLikeMeasure
    def compare(that: BMLikeMeasure) = this.score.compare(that.score)
    def score: Float = BMLikeMeasure.compute(frequency, docLength)
    def getSerializer = new BMLikeMeasureSerializer()
}

class BMLikeMeasureSerializer extends MeasureSerializerBase {
    val numberOfComponentsRequired = 2
    val compressFlags = List(true,true)
    def numberOfBytesRequired = 6

    def MinValue: Measure = BMLikeMeasure(0,1)

    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float =
        if(components.length != numberOfComponentsRequired + 1)
            throw new IllegalArgumentException("wrong number of components")
        else {
            val frequency = components(1)(measurePosition)
            val norm = components(2)(measurePosition)
            BMLikeMeasure.compute(frequency, norm)
        }

    def serialize(_m: Measure, buf: ByteBuffer) = {
        val m = _m.asInstanceOf[BMLikeMeasure]
        buf.putShort(m.frequency) //because it writes 8 lowest bits
        buf.putInt(m.docLength)
    }
    def deserialize(buf: ByteBuffer): Measure = {
        val freq = buf.getShort()
        val norm = buf.getInt()
        return BMLikeMeasure(freq,norm)
    }
    //component 0 - is used for doc Ids
    def writeToComponents(_m: Measure, components: Array[Array[Int]], i: Int): Unit = {
        val m = _m.asInstanceOf[BMLikeMeasure]
        components(1)(i) = m.frequency.asInstanceOf[Int]
        components(2)(i) = m.docLength
    }
    def readFromComponents(components: Array[Array[Int]], i: Int): Measure =
        BMLikeMeasure(components(1)(i).asInstanceOf[Short], components(2)(i))
}
