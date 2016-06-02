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
