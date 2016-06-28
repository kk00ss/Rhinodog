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

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.sun.xml.internal.messaging.saaj.util.{ByteInputStream, ByteOutputStream}
import org.roaringbitmap.buffer.MutableRoaringBitmap
import org.slf4j.LoggerFactory

object BitSetSegment {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def deserialize(data: Array[Byte]): BitSetSegment = {
        val restoredBitset = new MutableRoaringBitmap()
        restoredBitset.deserialize(new DataInputStream(
            new ByteInputStream(data, data.length)))
        return BitSetSegment(restoredBitset)
    }
}
/* all operation are thread safe */
case class BitSetSegment
(bitSet: MutableRoaringBitmap = new MutableRoaringBitmap()) {

    private val lock: ReentrantReadWriteLock = new ReentrantReadWriteLock(true)

    def add(docID: Int) = {
        BitSetSegment.logger.trace("-> add")
        BitSetSegment.logger.debug("add docID = {}", docID)
        lock.writeLock().lock()
        bitSet.add(docID)
        lock.writeLock().unlock()
        BitSetSegment.logger.trace("add ->")
    }
    def check(docID: Int): Boolean = {
        BitSetSegment.logger.trace("-> check")
        BitSetSegment.logger.debug("check docID = {}", docID)
        lock.readLock().lock()
        val ret = bitSet.contains(docID)
        lock.readLock().unlock()
        BitSetSegment.logger.trace("check ->")
        return ret
    }
    def serialize(): Array[Byte] = {
        BitSetSegment.logger.trace("-> serialize")
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
            case ex: Exception => BitSetSegment.logger.error("!!! serialize", ex)
        } finally {
            lock.readLock().unlock()
        }
        BitSetSegment.logger.trace("serialize ->")
        return ret
    }
}
