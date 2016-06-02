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

import java.nio.ByteBuffer

import org.junit._
import rhinodog.Core.MeasureFormats._

class MeasureSimpleTest {
    @Test
    def compareTest(): Unit = {
        val a = MeasureSimple(1,2)
        val b = MeasureSimple(1,3)
        assert(a.compare(b) > 0)
    }
    val serializer = new MeasureSerializerSimple()
    @Test
    def serializationTest(): Unit = {
        val m = MeasureSimple(-128, 1580)
        val buf = ByteBuffer.allocate(serializer.numberOfBytesRequired)
        serializer.serialize(m, buf)
        val m1 = serializer.deserialize(ByteBuffer.wrap(buf.array()))
        assert(m == m1)
    }
    @Test
    def componentsSerializationTest(): Unit = {
        // component 0 is used for DocIDs
        val components = (0 to serializer.numberOfComponentsRequired)
            .toArray.map(i => new Array[Int](1))
        val m = MeasureSimple(-128, 1580)
        serializer.writeToComponents(m, components, 0)
        val m1 = serializer.readFromComponents(components, 0)
        assert(m == m1)
    }
}
