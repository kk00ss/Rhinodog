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
package rhinodog.Core.Utils

import rhinodog.Core.Definitions._
import java.nio.ByteBuffer


object MetadataSerializer {
    val instances = new java.util.concurrent.ConcurrentHashMap[String, MetadataSerializer]()

    def apply
    (measureSerializer: MeasureSerializerBase): MetadataSerializer = {
        val className = measureSerializer.getClass.getTypeName
        if(!instances.containsKey(className))
            instances.putIfAbsent(className, new MetadataSerializer(measureSerializer))
        return instances.get(className)
    }
}

class MetadataSerializer private() {
    private def this(m: MeasureSerializerBase) = {
        this()
        this.measureSerializer = m
    }
    private var measureSerializer: MeasureSerializerBase = null

    def serialize(meta: BlockMetadata): BlockMetadataSerialized = {
        val buf = ByteBuffer.allocate(10+measureSerializer.numberOfBytesRequired)
        measureSerializer.serialize(meta.maxMeasureValue,buf)
        buf.putInt(meta.encodedSize)
        buf.putInt(meta.totalNumber)
        buf.put(meta.changeByte)
        buf.put(meta.compactionLevel)
        return buf.array()
    }

    def deserialize(buf: ByteBuffer): BlockMetadata = {
        val maxMeasureValue = measureSerializer.deserialize(buf)
        val encodedSize = buf.getInt()
        val totalNumber = buf.getInt()
        val changeByte = buf.get()
        val compactionLevel = buf.get()
        return BlockMetadata(maxMeasureValue, encodedSize, totalNumber, changeByte, compactionLevel)
    }
}
