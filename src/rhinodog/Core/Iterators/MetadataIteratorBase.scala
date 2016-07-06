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
package rhinodog.Core.Iterators

import rhinodog.Core.Definitions._
import BaseTraits.ISnapshotReader

import scala.collection.immutable.TreeMap
import scala.collection.mutable

trait MetadataIteratorBase {
    var currentElement: (Long, BlockInfoBase)
    def hasNext: Boolean
    def next(): Long
    def advance(docID: Long): Long
    def close()
}

class MetaIteratorRAM
(blocks: TreeMap[Long, BlockInfoRAM]) extends MetadataIteratorBase {
    private var iterator = blocks.iterator
    var currentElement: (Long, BlockInfoBase) = null
    next()

    def hasNext = iterator.hasNext
    def next(): Long =  {
        if(iterator.hasNext)
            currentElement = iterator.next()
        else currentElement = (-1, null)
        return currentElement._1
    }
    def advance(docID: Long): Long = {
        iterator = blocks.iteratorFrom(docID) //blocks.dropWhile(_.maxDocID < docID).iterator
        if(iterator.hasNext)
            currentElement = iterator.next()
        else currentElement = (-1, null)
        return currentElement._1
    }
    def close() = {}
}

class MetaIterator
(termID: Int,
 meta: TermMetadata,
 snapshot: ISnapshotReader) extends MetadataIteratorBase {
    private var iterator = meta.blocks.iterator
    var currentElement: (Long, BlockInfoBase) = null
    next()

    def hasNext = iterator.hasNext
    def next(): Long =  {
        val (blockKey, blockMeta) = iterator.next()
        currentElement = (blockKey.maxDocID, BlockInfo(blockKey,blockMeta,snapshot))
        return currentElement._1
    }
    def advance(docID: Long): Long = {
        iterator = meta.blocks.iteratorFrom(BlockKey(termID,docID))
        if(iterator.hasNext) {
            val (blockKey, blockMeta) = iterator.next()
            currentElement = (blockKey.maxDocID, BlockInfo(blockKey, blockMeta, snapshot))
        } else currentElement = (-1, null)
        return currentElement._1
    }
    def close() = { snapshot.close() }
}
