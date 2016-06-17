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

import rhinodog.Core.Definitions.{Measure,MeasureSerializerBase}

object NormalizedFloat16Bit {
    val range = 65535
    val step = 1f / range
    def convertFloatToShort(value: Float): Short = {
        val newValue = math.round(value / step)
        (if(newValue > Short.MaxValue) (newValue & 0x7FFF) * -1
        else newValue).asInstanceOf[Short]
    }

    def convertShortToFloat(value: Int): Float = {
        val actualValue = if(value < 0) value + (1 << 15) else value
        step * actualValue
    }
}

//with loss of precision, only the rangoe of 0-1 is supported
class NormalizedFloat16Bit(input: Float) extends Measure {
    if(input > 1f) throw new IllegalArgumentException("only values in the range 0-1 are supported")

    def this(shortValue: Short) = this(NormalizedFloat16Bit.convertShortToFloat(shortValue))

    type Self = NormalizedFloat16Bit

    lazy val shortValue: Short = NormalizedFloat16Bit.convertFloatToShort(input)
    lazy val score: Float = NormalizedFloat16Bit.convertShortToFloat(shortValue)

    def compare(that: NormalizedFloat16Bit) = this.score.compare(that.score)
    def getSerializer = new Float16BitSerializer()
}

class Float16BitSerializer extends MeasureSerializerBase {
    val numberOfComponentsRequired = 1
    val compressFlags = List(false)

    //TODO: add parameter to disable compression of components
    //TODO: OR find a way to get exponent and mantisa out of Int representation of Float
    def MinValue: Measure = new NormalizedFloat16Bit(0)

    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float =
        if (components.length != numberOfComponentsRequired + 1)
            throw new IllegalArgumentException("wrong number of components")
        else NormalizedFloat16Bit.convertShortToFloat(components(1)(measurePosition))

    def numberOfBytesRequired = 2
    def serialize(_m: Measure, buf: ByteBuffer) = buf.putShort(_m.asInstanceOf[NormalizedFloat16Bit].shortValue)
    def deserialize(buf: ByteBuffer): Measure = MeasureFloat(buf.getShort)

    //component 0 - is used for doc Ids
    def writeToComponents(_m: Measure, components: Array[Array[Int]], i: Int)
    =components(1)(i) = _m.asInstanceOf[NormalizedFloat16Bit].shortValue

    def readFromComponents(components: Array[Array[Int]], i: Int)
    //second constructor must be used - thus conversion
    = new NormalizedFloat16Bit(components(1)(i).asInstanceOf[Short])
}
