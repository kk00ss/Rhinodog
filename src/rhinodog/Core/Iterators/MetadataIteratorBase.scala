package rhinodog.Core.Iterators

import rhinodog.Core.Definitions._
import BaseTraits.SnapshotReaderInterface

import scala.collection.immutable.TreeMap
import scala.collection.mutable

trait MetadataIteratorBase {
    var currentElement: (Long, BlockInfoBase)
    def hasNext: Boolean
    def next(): Long
    def advance(docID: Long): Long
}

class MetaIteratorRAM
(blocks: mutable.ArrayBuffer[BlockInfoRAM]) extends MetadataIteratorBase {
    private var iterator = blocks.iterator
    var currentElement: (Long, BlockInfoBase) = null
    next()

    def hasNext = iterator.hasNext
    def next(): Long =  {
        if(iterator.hasNext) {
            val tmp = iterator.next()
            currentElement = (tmp.maxDocID, tmp)
        }
        else currentElement = (-1, null)
        return currentElement._1
    }
    def advance(docID: Long): Long = {
        iterator = blocks.dropWhile(_.maxDocID < docID).iterator
        if(iterator.hasNext) {
            val tmp = iterator.next()
            currentElement = (tmp.maxDocID, tmp)
        }
        else currentElement = (-1, null)
        return currentElement._1
    }
}

class MetaIterator
(termID: Int,
 meta: TermMetadata,
 snapshot: SnapshotReaderInterface) extends MetadataIteratorBase {
    private var iterator = meta.blocks.iterator
    var currentElement: (Long, BlockInfoBase) = null
    next()

    def hasNext = iterator.hasNext
    def next(): Long =  {
        val (blockKey, blockMeta) = iterator.next()
        if(iterator.hasNext)
            currentElement = (blockKey.maxDocID, BlockInfo(blockKey,blockMeta,snapshot))
        else currentElement = (-1, null)
        return currentElement._1
    }
    def advance(docID: Long): Long = {
        iterator = meta.blocks.iteratorFrom(BlockKey(termID,docID))
        val (blockKey, blockMeta) = iterator.next()
        if(iterator.hasNext)
            currentElement = (blockKey.maxDocID, BlockInfo(blockKey,blockMeta,snapshot))
        else currentElement = (-1, null)
        return currentElement._1
    }
}