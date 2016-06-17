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
package rhinodog.Core.Definitions

import java.nio.ByteBuffer

trait Measure {
    type Self <: Measure
    def compare(that: Self): Int
    def score: Float
    def getSerializer: MeasureSerializerBase
}

trait MeasureSerializerBase {
    /* Each component gives one Int32 value for serialization,
        each component is compressed separately
        so Long takes 2 components.
        Dynamic length data types for Measure are not supported */
    val numberOfComponentsRequired: Int
    val compressFlags: Seq[Boolean]
    def numberOfBytesRequired: Int
    def scoreFromComponents(components: Array[Array[Int]], measurePosition: Int): Float
    def serialize(m: Measure, buf: ByteBuffer)
    def deserialize(buf: ByteBuffer): Measure
    def writeToComponents(m: Measure, components: Array[Array[Int]], i: Int): Unit
    def readFromComponents(components: Array[Array[Int]], i: Int): Measure
    def MinValue: Measure
}
