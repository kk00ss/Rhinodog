package rhinodog.Core.MeasureFormats

import java.nio.ByteBuffer

import rhinodog.Core.Definitions._

case class MeasureSimple(frequency: Byte, norm: Int) extends Measure {
    type Self = MeasureSimple
    def compare(that: MeasureSimple) = this.score.compare(that.score)
    def numberOfBytesRequired = 5
    def score = (frequency + 0f) / norm
    def getSerializer = new MeasureSerializerSimple()
}

class MeasureSerializerSimple extends MeasureSerializerBase {
    def numberOfComponentsRequired = 2
    def numberOfBytesRequired = 5

    def MinValue: Measure = MeasureSimple(0,1)

    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float =
        if(components.length != numberOfComponentsRequired + 1)
            throw new IllegalArgumentException("wrong number of components")
        else components(1)(measurePosition).asInstanceOf[Float]/components(2)(measurePosition)

    def serialize(_m: Measure, buf: ByteBuffer) = {
        val m = _m.asInstanceOf[MeasureSimple]
        buf.put(m.frequency) //because it writes 8 lowest bits
        buf.putInt(m.norm)
    }
    def deserialize(buf: ByteBuffer): Measure = {
        val freq = buf.get()
        val norm = buf.getInt()
        return MeasureSimple(freq,norm)
    }
    //component 0 - is used for doc Ids
    def writeToComponents(_m: Measure, components: Array[Array[Int]], i: Int): Unit = {
        val m = _m.asInstanceOf[MeasureSimple]
        components(1)(i) = m.frequency.asInstanceOf[Int]
        components(2)(i) = m.norm
    }
    def readFromComponents(components: Array[Array[Int]], i: Int): Measure =
        MeasureSimple(components(1)(i).asInstanceOf[Byte], components(2)(i))
}
