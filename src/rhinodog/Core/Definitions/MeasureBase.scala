package rhinodog.Core.Definitions

import java.nio.ByteBuffer

trait Measure {
    type Self <: Measure
    def compare(that: Self): Int
    def numberOfBytesRequired: Int
    def score: Float
    def getSerializer: MeasureSerializerBase
}

trait MeasureSerializerBase {
    /* Each component gives one Int32 value for serialization,
        each component is compressed separately
        so Long takes 2 components.
        Dynamic length data types for Measure are not supported */
    def numberOfComponentsRequired: Int
    def numberOfBytesRequired: Int
    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float
    def serialize(m: Measure, buf: ByteBuffer)
    def deserialize(buf: ByteBuffer): Measure
    def writeToComponents(m: Measure, components: Array[Array[Int]], i: Int): Unit
    def readFromComponents(components: Array[Array[Int]], i: Int): Measure
    def MinValue: Measure
}