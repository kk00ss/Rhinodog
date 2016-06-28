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

import java.nio.ByteBuffer

import org.slf4j.LoggerFactory
import rhinodog.Core.Definitions._

import scala.collection._

import me.lemire.integercompression._

//TODO:Integrate TurboPFor
object IntegerEncoder {
    val codec = new SkippableComposition(new BinaryPacking(), new VariableByte())
    val intCompressor = new IntCompressor(codec)

    def decodeInt(compressed: Array[Int]): Array[Int] = {
        val uncompressed = intCompressor.uncompress(compressed)
        uncompressed
    }

    def encodeInt(data: Array[Int]): Array[Int] = {
        val compressed = intCompressor.compress(data)
        compressed
    }
}

object DocPostingsSerializer {
    val segmentSize = 128

    def framingSizeInBytes(numComponents: Int): Int = 8 + numComponents * 4

    def sizeInBytes(compressedComponents: SegmentSerialized): Int
    = 8 + compressedComponents.length * 4 +
        compressedComponents.foldLeft(0)((x, y) => x + y.length) * 4

    def writeComponents(compressedComponents: SegmentSerialized, buf: ByteBuffer) = {
        buf.putInt(compressedComponents.length)
        val placeholder = buf.position()
        buf.putInt(0)

        var i = 0
        while (i < compressedComponents.length) {
            buf.putInt(compressedComponents(i).length)
            var j = 0
            while (j < compressedComponents(i).length) {
                buf.putInt(compressedComponents(i)(j))
                j += 1
            }
            i += 1
        }

        val end = buf.position()
        val length = buf.position() - placeholder - 4
        buf.position(placeholder)
        buf.putInt(length)
        buf.position(end)
    }

    def readComponents(buf: ByteBuffer): SegmentSerialized = {
        val componentsLength = buf.getInt
        val length = buf.getInt
        val start = buf.position()
        val components = new Array[Array[Int]](componentsLength)
        var i = 0
        while (i < componentsLength && (buf.position < start + length)) {
            val componentLength = buf.getInt
            val component = new Array[Int](componentLength)
            var j = 0
            while (j < componentLength) {
                component(j) = buf.getInt
                j += 1
            }
            //components(i) = IntegerEncoder.decodeInt(component)
            components(i) = component
            i += 1
        }
        return components
    }

    def readIntoBuffer(buf: ByteBuffer,
                       components: SegmentSerialized,
                       lengths: Array[Int]): Unit = {
        val componentsLength = buf.getInt
        val length = buf.getInt
        val start = buf.position()
        var i = 0
        while (i < componentsLength && (buf.position < start + length)) {
            val componentLength = buf.getInt
            lengths(i) = componentLength
            var j = 0
            while (j < componentLength) {
                components(i)(j) = buf.getInt
                j += 1
            }
            i += 1
        }
    }

    def decodeComponents(components: SegmentSerialized,
                         compressFlags: Seq[Boolean]): SegmentSerialized = {
        val decompressedComponents = new SegmentSerialized(components.length)
        var i = 0
        while(i < components.length) {
            val c = components(i)
            val flag = if(i ==0) true else compressFlags(i-1)
            decompressedComponents(i) = if (flag) IntegerEncoder.decodeInt(c) else c
            i += 1
        }
        return decompressedComponents
    }

    def decodeIntoBuffer(components: SegmentSerialized,
                         compressFlags: Seq[Boolean],
                         buffer: SegmentSerialized,
                         lengths: Array[Int]): Int = {
        if(components.length != buffer.length ||
            buffer.head.length < segmentSize * 3)
            throw new IllegalArgumentException("wrong buffer size")

        val maxDocIDsOffset = new IntWrapper()
        IntegerEncoder.codec.headlessUncompress(
            components(0),
            new IntWrapper(1),
            lengths(0) - 1,
            buffer(0),
            maxDocIDsOffset,
            components(0)(0))

        components.indices.drop(1).foreach( i => {
            val flag = if(i ==0) true else compressFlags(i-1)
            if(flag) {
                val maxOffset = new IntWrapper()
                IntegerEncoder.codec.headlessUncompress(
                    components(i),
                    new IntWrapper(1),
                    lengths(i) - 1,
                    buffer(i),
                    maxOffset,
                    components(i)(0))
            } else components(i).copyToArray(buffer(i))
        })

        return maxDocIDsOffset.get()
    }

    def encodeComponents(components: SegmentSerialized,
                         compressFlags: Seq[Boolean]): SegmentSerialized = {
        val compressedComponents = new SegmentSerialized(components.length)
        var i = 0
        while(i < components.length) {
            val c = components(i)
            val flag = if(i ==0) true else compressFlags(i-1)
            compressedComponents(i) = if (flag) IntegerEncoder.encodeInt(c) else c
            i += 1
        }
        return compressedComponents
    }
}

//TODO: 3 Use TurboPFor ### requires linux or some advanced C++ hacking
class DocPostingsSerializer
(measureSerializer: MeasureSerializerBase) {

    def encodeIntoComponents(data: Seq[DocPosting]): SegmentSerialized = {
        if(data.nonEmpty) {
        val numOfComponents = measureSerializer.numberOfComponentsRequired
        //one for docID
        val components = new Array[Array[Int]](numOfComponents + 1)
        for (i <- 1 to numOfComponents)
            components(i) = new Array[Int](data.length)
        val inputDocIDs = new mutable.ArrayBuffer[Int](data.length)
        //val inputFrequencies = new Array[Int](data.length)
        //val inputNorms = new Array[Int](data.length)
        var i = 0
        var prevDocId = 0l
        while (i < data.length) {
            val diff = data(i).docID - prevDocId
            if(diff <= 0)
                throw new IllegalArgumentException("postings should be sorted by docID")
            //diff is always > 0 - no need to encode it
            if (diff > Int.MaxValue /*|| diff == 0*/ ) {
                inputDocIDs += 0
                inputDocIDs += (diff / Int.MaxValue).asInstanceOf[Int]
                inputDocIDs += (diff % Int.MaxValue).asInstanceOf[Int]
            } else {
                inputDocIDs += diff.asInstanceOf[Int]
            }
            prevDocId = data(i).docID
            measureSerializer.writeToComponents(data(i).measure, components, i)
            i += 1
        }
        components(0) = inputDocIDs.toArray
        //saving metadata/framing protocol
            return DocPostingsSerializer.encodeComponents(components,
                measureSerializer.compressFlags)
        } else return new SegmentSerialized(0)
    }

    def decodeFromComponents(compressed: SegmentSerialized): Array[DocPosting] = {
        val components = DocPostingsSerializer.decodeComponents(compressed,
            measureSerializer.compressFlags)
        if (components.length != 3 ||
            components(1).length != components(2).length ||
            components(0).length < components(1).length)
            throw new scala.IllegalArgumentException("buffer is corrupted")
        val dataLength = components(1).length
        val ret = new Array[DocPosting](dataLength)

        var i = 0
        var j = 0
        while (i < dataLength) {
            var docID = if (i > 0) ret(i - 1).docID else 0l
            if (components(0)(j) != 0) {
                docID += components(0)(j)
                j += 1
            } else {
                docID += components(0)(j + 1) * Int.MaxValue.asInstanceOf[Long] + components(0)(j + 2)
                j += 3
            }
//            if (components(1)(i) > Byte.MaxValue)
//                throw new scala.IllegalArgumentException("buffer is corrupted")
            ret(i) = new DocPosting(docID, measureSerializer.readFromComponents(components, i))
            i += 1
        }
        return ret
    }
}
