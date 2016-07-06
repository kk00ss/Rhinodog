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

import org.slf4j.LoggerFactory

import scala.collection.Seq
import scala.collection.JavaConversions.mapAsScalaMap

import rhinodog.Core.Definitions._
import BaseTraits._
import Configuration._
import rhinodog.Core.Utils._

import java.util.concurrent._

/*Thread safety of flush is not guarantied*/
case class MetadataManager(measureSerializer: MeasureSerializerBase)
    extends IMetadataManager {
    private val logger = LoggerFactory.getLogger(this.getClass)

    //========= METADATA STATE =================================================
    //TermID -> TermMetadata
    private val metadataByTerm = new ConcurrentHashMap[Int, TermMetadata]()
    private val bitSetSegments = new ConcurrentSkipListMap[Long, BitSetSegment]()
    //========= MetadataToFlush =======================================================
    private var changedSegments: ConcurrentHashMap[Long, BitSetSegment] = new ConcurrentHashMap()
    private var changedDeletionInfo: ConcurrentHashMap[BlockKey, Int] = new ConcurrentHashMap()

    def compare(other: MetadataManager): Boolean = {
        var bitSetsTest = true
        for (pair <- bitSetSegments)
            if (other.bitSetSegments.containsKey(pair._1)) {
                if (other.bitSetSegments(pair._1).bitSet != pair._2.bitSet)
                    bitSetsTest = false
            } else bitSetsTest = false
        return bitSetsTest && other.metadataByTerm == this.metadataByTerm
    }

    //========= Utils =================================================================
    val serializer = MetadataSerializer(measureSerializer)
    var mergeDetector: ICompactionManager = null

    /* 0 indicates that term metadata is absent */
    override def getNumberOfDocs(termID: Int): Long = {
        val meta = metadataByTerm.get(termID)
        val ret = if (meta != null) meta.numberOfDocs
        else 0
        logger.trace("getNumberOfDocs termID = {} nDocs = {}", termID, ret)
        ret
    }

    override def getMostFrequentTerms(num: Int): Array[Int] =
        metadataByTerm.toArray
            .sortWith(_._2.numberOfDocs > _._2.numberOfDocs)
            .map(_._1)
            .take(num)

    private val restoreNodeMetadata =
        (key: BlockKey, buffer: ByteBuffer) => {
            var termMetadata = metadataByTerm.get(key.termID)
            if (termMetadata == null) {
                termMetadata = TermMetadata()
                metadataByTerm.put(key.termID, termMetadata)
            }
            val meta = serializer.deserialize(buffer)
            val newElement = (key, meta)
            termMetadata.blocks += newElement
            //we don't use live docs because numberOfDeleted is not read here
            termMetadata.numberOfDocs += meta.totalNumber
            logger.trace("restoreNodeMetadata termID = {} early estimate on nDocs = {}",
                key.termID, termMetadata.numberOfDocs)
            if (mergeDetector != null)
                mergeDetector.blockAddedEvent(key.termID, key, meta)
        }
    private val restoreNumDeleted = (key: BlockKey, numDeleted: Int) => {
        val termMetadata = metadataByTerm.get(key.termID)
        if (termMetadata != null) {
            val tmp = termMetadata.blocks.get(key)
            if(tmp.isEmpty) {
                val ex = new IllegalStateException("no term metadata for blockKey")
                logger.error("restoreNumDeleted",ex)
                throw ex
            }
            val blockMetadata = tmp.get
            blockMetadata.numberOfDeleted = numDeleted
            termMetadata.numberOfDocs -= numDeleted
            logger.trace("restoreNodeMetadata termID = {} early estimate on nDocs = {}",
                key.termID, termMetadata.numberOfDocs)
        }
    }

    private val restoreBitSet: (BitSetSegmentSerialized) => Unit =
        (bitset) => {
            logger.trace("restoreBitSet key = {}", bitset.key)
            bitSetSegments.put(bitset.key, BitSetSegment.deserialize(bitset.data))
        }


    val restoreMetadataHook = RestoreMetadataHook(restoreNodeMetadata, restoreNumDeleted, restoreBitSet)

    //========== METADATA MANAGEMENT ===================================================
    /* returns key: Long, docIDinRange: Int */
    private def bitSetSegmentKey(docID: Long): (Long, Int) = {
        val key = docID / GlobalConfig.storage_bitSetSegmentRange
        val docIDinRange = (docID % GlobalConfig.storage_bitSetSegmentRange).asInstanceOf[Int]
        logger.trace("bitSetSegmentKey docID = {}, key = {}, docIDinRange = {}",
            Array(docID,key,docIDinRange))
        return (key, docIDinRange)
    }

    override def isDeleted(docID: Long): Boolean = {
        var ret = false
        if (bitSetSegments.nonEmpty) {
            val (key, docIDinRange) = bitSetSegmentKey(docID)
            val bitSetSegment = bitSetSegments.get(key)
            if (bitSetSegment != null)
                ret =  bitSetSegment.check(docIDinRange)
        }
        logger.trace("isDeleted docID = {}, isDeleted = {}",docID,ret)
        return ret
    }

    override def markDeleted(docID: Long): Unit = {
        logger.trace("isDeleted docID = {}", docID)
        val (key, docIDinRange) = bitSetSegmentKey(docID)
        var segment = bitSetSegments.get(key)
        if (segment == null) bitSetSegments.putIfAbsent(key, BitSetSegment())
        segment = bitSetSegments.get(key)
        segment.synchronized {
            segment.add(docIDinRange)
        }
        changedSegments.put(key, segment)
    }

    /*Thread safety is controlled in Storage, should be executed with TermWriterLock*/
    override def deleteFromTerm(termID: Int, docID: Long): Boolean = {
        logger.trace("deleteFromTerm termID = {}, docID = {}", termID, docID)
        val termMetadata = metadataByTerm.get(termID)
        if (termMetadata == null) return false
        val key = BlockKey(termID, docID, 0)
        val blockMetadata = termMetadata.blocks.from(key).headOption
        if (blockMetadata.isEmpty) return false
        val actualBlockMD = blockMetadata.get
        termMetadata.numberOfDocs -= 1
        actualBlockMD._2.numberOfDeleted += 1
        val blockKey = actualBlockMD._1
        changedDeletionInfo.put(blockKey, actualBlockMD._2.numberOfDeleted)
        if (mergeDetector != null)
            mergeDetector.docDeletedEvent(blockKey, actualBlockMD._2)
        return true
    }

    override def getTermMetadata(termID: Int): Option[TermMetadata] = {
        logger.debug("getTermMetadata termID = {}", termID)
        val termData = metadataByTerm.get(termID)
        if (termData != null) return Some(termData)
        else None
    }

    /*Thread safety is controlled in Storage, should be executed with TermWriterLock*/
    override def addMetadata(key: BlockKey,
                             meta: BlockMetadata): TermMetadata = {
        logger.debug("-> addMetadata key = {}",key)
        logger.trace("addMetadata meta = {}",meta)
        var termMetadata = metadataByTerm.get(key.termID)
        if (termMetadata == null) {
            termMetadata = TermMetadata()
            metadataByTerm.put(key.termID, termMetadata)
        }
        termMetadata.blocks += ((key, meta))
        termMetadata.numberOfDocs += meta.totalNumber
        if (mergeDetector != null)
            mergeDetector.blockAddedEvent(key.termID, key, meta)
        return termMetadata
    }

    //executed only as part of GC JOB
    /*Thread safety is controlled in Storage, should be executed with TermWriterLock*/
    override def replaceMetadata(termID: Int,
                                 deleted: Seq[BlockKey],
                                 added: Seq[(BlockKey, BlockMetadata)]) = {
        logger.debug("-> replaceMetadata termID = {}, deleted = {}",termID, deleted)
        logger.trace("replaceMetadata added = {}",added)
        val newTermMetadata = metadataByTerm.get(termID).copy()
        for (key <- deleted) if (newTermMetadata.blocks.contains(key))
            newTermMetadata.blocks -= key
        for (newMeta <- added)
            newTermMetadata.blocks += newMeta
        metadataByTerm.put(termID, newTermMetadata)
    }

    override def flush: MetadataToFlush = {
        var ret: MetadataToFlush = null
        val segments = changedSegments.map(segment => {
            BitSetSegmentSerialized(segment._1, segment._2.serialize())
        }).toArray
        ret = MetadataToFlush(segments, changedDeletionInfo)
        if(logger.isTraceEnabled)
            logger.trace("flush MetadataToFlush changedDeletionInfo = {}", changedDeletionInfo.toList)
        changedSegments = new ConcurrentHashMap()
        changedDeletionInfo = new ConcurrentHashMap()
        return ret
    }
}
