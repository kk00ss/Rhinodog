package rhinodog.Core.Utils

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.sun.xml.internal.messaging.saaj.util.{ByteInputStream, ByteOutputStream}
import org.roaringbitmap.buffer.MutableRoaringBitmap

object BitSetSegment {
    def deserialize(data: Array[Byte]): BitSetSegment = {
        val restoredBitset = new MutableRoaringBitmap()
        restoredBitset.deserialize(new DataInputStream(
            new ByteInputStream(data, data.length)))
        return BitSetSegment(restoredBitset)
    }
}
/* all operation are thread safe */
case class BitSetSegment
(bitSet: MutableRoaringBitmap = new MutableRoaringBitmap(),
 private val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock(true)) {

    def add(docID: Int) = {
        lock.writeLock().lock()
        bitSet.add(docID)
        lock.writeLock().unlock()
    }
    def check(docID: Int): Boolean = {
        lock.readLock().lock()
        val ret = bitSet.contains(docID)
        lock.readLock().unlock()
        return ret
    }
    def serialize(): Array[Byte] = {
        lock.readLock().lock()
        var ret: Array[Byte] = null
        try {
            bitSet.runOptimize()
            val byteOutStream = new ByteOutputStream(bitSet.serializedSizeInBytes())
            val dataOutput = new DataOutputStream(new BufferedOutputStream(byteOutStream))
            bitSet.serialize(dataOutput)
            dataOutput.flush()
            ret = byteOutStream.getBytes
        } catch {
            case ex: Exception => {
                ex.printStackTrace()
            }
        } finally {
            lock.readLock().unlock()
        }
        return ret
    }
}
