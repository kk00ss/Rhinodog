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
package rhinodog.Core

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import scala.collection.Seq
import scala.collection.JavaConversions.mapAsScalaMap

import rhinodog.Core.Definitions._
import BaseTraits._
import Configuration._
import rhinodog.Core.Utils._

import java.util.concurrent._

/*Thread safety of flush is not guarantied*/
case class MetadataManager
(globalConfig: GlobalConfig,
 measureSerializer: MeasureSerializerBase)
    extends MetadataManagerBase {
    //========= METADATA STATE =================================================
    //TermID -> TermMetadata
    private val metadataByTerm = new ConcurrentHashMap[Int, TermMetadata]()
    private val bitSetSegments = new ConcurrentSkipListMap[Long, BitSetSegment]()
    //========= MetadataToFlush =======================================================
    private var changedSegments: ConcurrentHashMap[Long, BitSetSegment] = new ConcurrentHashMap()
    private var changedDeletionInfo: ConcurrentHashMap[BlockKey, Int] = new ConcurrentHashMap()

    def compare(other: MetadataManager): Boolean = {
        var bitSetsTest = true
        for(pair <- bitSetSegments)
            if(other.bitSetSegments.containsKey(pair._1)) {
                if (other.bitSetSegments(pair._1).bitSet != pair._2.bitSet)
                    bitSetsTest = false
            } else bitSetsTest = false
        return bitSetsTest && other.metadataByTerm == this.metadataByTerm
    }
    //========= Utils =================================================================
    val serializer = MetadataSerializer(measureSerializer)
    var mergeDetector: CompactionManagerInterface = null

    /* 0 indicates that term metadata is absent */
    override def getNumberOfDocs(termID: Int): Long = {
        val meta = metadataByTerm.get(termID)
        if(meta != null) return meta.numberOfDocs
        else return 0
    }

    private val restoreNodeMetadata =
            (key: BlockKey, buffer: ByteBuffer) => {
                var termMetadata = metadataByTerm.get(key.termID)
                if(termMetadata == null) {
                    termMetadata = TermMetadata()
                    metadataByTerm.put(key.termID, termMetadata)
                }
                val meta = serializer.deserialize(buffer)
                val newElement = (key, meta)
                termMetadata.blocks += newElement
                //we don't use live docs because numberOfDeleted is not read here
                termMetadata.numberOfDocs += meta.totalNumber
                if(mergeDetector != null)
                    mergeDetector.blockAddedEvent(key.termID, key, meta)
        }
    private val restoreNumDeleted = (key: BlockKey, numDeleted: Int) => {
        val termMetadata = metadataByTerm.get(key.termID)
        if(termMetadata != null) {
            val blockMetadata = termMetadata.blocks.get(key)
            if (blockMetadata.isDefined) {
                blockMetadata.get.numberOfDeleted = numDeleted
                termMetadata.numberOfDocs -= numDeleted
            }
        }
    }

    private val restoreBitSet: (BitSetSegmentSerialized) => Unit =
        (bitset) => bitSetSegments.put(bitset.key, BitSetSegment.deserialize(bitset.data))


    val restoreMetadataHook = RestoreMetadataHook(restoreNodeMetadata,restoreNumDeleted,restoreBitSet)

    //========== METADATA MANAGEMENT ===================================================
    /* returns key: Long, docIDinRange: Int */
    private def bitSetSegmentKey(docID: Long): (Long, Int) = {
        val key = docID / globalConfig.bitSetSegmentRange
        val docIDinRange = (docID % globalConfig.bitSetSegmentRange).asInstanceOf[Int]
        return (key, docIDinRange)
    }

    override def isDeleted(docID: Long): Boolean = {
        if(bitSetSegments.isEmpty)
            return false
        val (key, docIDinRange) = bitSetSegmentKey(docID)
        val bitSetSegment = bitSetSegments.get(key)
        if(bitSetSegment == null)
            return false
        return bitSetSegment.check(docIDinRange)
    }
    override def markDeleted(docID: Long): Unit = {
        val (key, docIDinRange) = bitSetSegmentKey(docID)
        var segment = bitSetSegments.get(key)
        if(segment == null) bitSetSegments.putIfAbsent(key, BitSetSegment())
        segment = bitSetSegments.get(key)
        segment.synchronized {
            segment.add(docIDinRange)
        }
        changedSegments.put(key, segment)
    }

    /*Thread safety is controlled in Storage, should be executed with TermWriterLock*/
    override def deleteFromTerm(termID: Int, measure: Measure, docID: Long): Boolean = {
        val termMetadata = metadataByTerm.get(termID)
        if(termMetadata == null) return false
        val key = BlockKey(termID, docID, 0)
        val blockMetadata = termMetadata.blocks.from(key).headOption
        if(blockMetadata.isEmpty) return false
        val actualBlockMD = blockMetadata.get
        termMetadata.numberOfDocs -= 1
        actualBlockMD._2.numberOfDeleted +=1
        val blockKey = actualBlockMD._1
        changedDeletionInfo.put(blockKey,  actualBlockMD._2.numberOfDeleted)
        if(mergeDetector != null)
            mergeDetector.docDeletedEvent(blockKey, actualBlockMD._2)
        return true
    }

    override def getTermMetadata(termID: Int): Option[TermMetadata] = {
        val termData = metadataByTerm.get(termID)
        if (termData != null) return Some(termData)
        else None
    }

    /*Thread safety is controlled in Storage, should be executed with TermWriterLock*/
    override def addMetadata(key: BlockKey,
                             meta: BlockMetadata): TermMetadata = {
        var termMetadata = metadataByTerm.get(key.termID)
        if(termMetadata == null) {
            termMetadata = TermMetadata()
            metadataByTerm.put(key.termID, termMetadata)
        }
        termMetadata.blocks += ((key, meta))
        termMetadata.numberOfDocs += meta.totalNumber
        if(mergeDetector != null)
            mergeDetector.blockAddedEvent(key.termID, key, meta)
        return termMetadata
    }

    //executed only as part of GC JOB
    /*Thread safety is controlled in Storage, should be executed with TermWriterLock*/
    override def replaceMetadata(termID: Int,
                                 deleted: Seq[BlockKey],
                                 added: Seq[(BlockKey, BlockMetadata)]) = {
        val newTermMetadata = metadataByTerm.get(termID).copy()
        for(key <- deleted) if(newTermMetadata.blocks.contains(key))
            newTermMetadata.blocks -= key
        for(newMeta <- added)
            newTermMetadata.blocks += newMeta
        metadataByTerm.put(termID, newTermMetadata)
    }

    override def flush: MetadataToFlush = {
        var ret: MetadataToFlush = null
        val segments = changedSegments.map(segment => {
            BitSetSegmentSerialized(segment._1, segment._2.serialize())
        }).toArray
        ret = MetadataToFlush(segments, changedDeletionInfo)
        changedSegments = new ConcurrentHashMap()
        changedDeletionInfo = new ConcurrentHashMap()
        return ret
    }
}
