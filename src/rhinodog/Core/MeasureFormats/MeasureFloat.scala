package rhinodog.Core.MeasureFormats

import java.nio.ByteBuffer

import rhinodog.Core.Definitions.{Measure,MeasureSerializerBase}

case class MeasureFloat(score: Float) extends Measure {
    type Self = MeasureFloat
    def compare(that: MeasureFloat) = this.score.compare(that.score)
    def numberOfBytesRequired = 5
    def getSerializer = new MeasureFloatSerializer()
}

class MeasureFloatSerializer extends MeasureSerializerBase {
    def numberOfComponentsRequired = 1
    //TODO: add parameter to disable compression of components
    //TODO: OR find a way to get exponent and mantisa out of Int representation of Float
    def MinValue: Measure = MeasureFloat(0)

    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float =
        if (components.length != numberOfComponentsRequired + 1)
            throw new IllegalArgumentException("wrong number of components")
        else java.lang.Float.intBitsToFloat(components(1)(measurePosition))

    def numberOfBytesRequired = 4
    def serialize(_m: Measure, buf: ByteBuffer) = buf.putFloat(_m.asInstanceOf[MeasureFloat].score)
    def deserialize(buf: ByteBuffer): Measure = MeasureFloat(buf.getFloat)

    private val cachedBuffer = ByteBuffer.allocate(4)

    //component 0 - is used for doc Ids
    def writeToComponents(_m: Measure, components: Array[Array[Int]], i: Int): Unit = {
        val score = _m.asInstanceOf[MeasureFloat].score
        components(1)(i) = java.lang.Float.floatToIntBits(score)
    }

    def readFromComponents(components: Array[Array[Int]], i: Int): Measure = {
        val score = java.lang.Float.intBitsToFloat(components(1)(i))
        MeasureFloat(score)
    }
}
