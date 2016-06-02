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

object BlockKey {
    val sizeInBytes: Byte = 13
    def deserialize(bytes: Array[Byte]): BlockKey = {
        val buf = ByteBuffer.wrap(bytes)
        return deserialize(buf)
    }
    def deserialize(buf: ByteBuffer): BlockKey = {
        val termID = buf.getInt
        val maxDocID = buf.getLong
        val changeByte = buf.get
        return BlockKey(termID, maxDocID, changeByte)
    }
}

case class BlockKey(termID:Int, maxDocID: Long, var changeByte: Byte = 0)
    extends Ordered[BlockKey] {
    def serialize: Array[Byte] = {
        val keyBuf = ByteBuffer.allocate(BlockKey.sizeInBytes)
        return this.serialize(keyBuf)
    }
    def serialize(keyBuf: ByteBuffer): Array[Byte] = {
        keyBuf.putInt(termID) // 4 bytes
        keyBuf.putLong(maxDocID) // 8 bytes
        keyBuf.put(changeByte) // 1 byte
        return keyBuf.array()
    }
    override def compare(that: BlockKey): Int = {
        val termC = termID.compare(that.termID)
        if(termC != 0) return termC
        val maxC = maxDocID.compare(that.maxDocID)
        if(maxC != 0) return maxC
        else return changeByte.compare(that.changeByte)
    }
}
