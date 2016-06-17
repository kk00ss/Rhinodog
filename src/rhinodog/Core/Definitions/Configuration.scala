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

import com.netflix.config.DynamicPropertyFactory
import rhinodog.Core.Definitions.BaseTraits._

import scala.collection.mutable

object Configuration {
    // Constants
    val KB = 1024
    val MB = 1024 * KB
    val GB = 1024 * MB

    object storageModeEnum
        extends Enumeration {
        type storageMode = this.Value
        val READ_ONLY, READ_WRITE, CREATE = this.Value
    }

    case class MainComponents
    (measureSerializer: MeasureSerializerBase,
     repository: IRepository,
     metadataManager: IMetadataManager)

    case class TermWriterConfig
    (termID: Int,
     mainComponents: MainComponents,
     targetSize: Int)

    object GlobalConfig {
        val propFactory = DynamicPropertyFactory.getInstance()
        // number of docIDs for which there will be single bitmap segment
        // smaller value means less efficient encoding, but less data to write on change
        def bitSetSegmentRange = propFactory.getIntProperty("bitSetSegmentRange", Short.MaxValue).get()
        //interval in milliseconds
        def smallFlushInterval = propFactory.getLongProperty("smallFlushInterval", 200).get()
        //number of small flushes per one large flush
        def largeFlushInterval = propFactory.getIntProperty("largeFlushInterval", 5).get()
        def pageSize = propFactory.getIntProperty("pageSize", 4*KB).get()
        def targetBlockSize: Int = pageSize - 16 // should always be pageSize - 16
        //LMDB folder will be path + '\'+"InvertedIndex"
        def path = propFactory.getStringProperty("path","storageFolder").get
        //storage space is acquired in chunks of this size
        def mapSizeIncreaseStep = propFactory.getIntProperty("mapSizeIncreaseStep", 1*GB).get()
        //since compaction should be all small, it's unclear for now which value is better
        def merges_maxConcurrent = propFactory.getIntProperty("merges.maxConcurrent", 4).get()
        //blockSize: Int = 256 * KB - 16, //don't need it because of partial reads
        //delay in ms - we need it to decouple compactions from analysis threads
        def merges_queueCheckInterval = propFactory.getLongProperty("merges.queueCheckInterval", 100).get()
        //async compactions will not start if the system CPU load is more than this
        def merges_cpuLoadThreshold = propFactory.getFloatProperty("merges.cpuLoadThreshold", 0.95f).get()
        // compactions are only performed to reclaim space used by deleted documents
        // there is no need for further consolidation of data if it is already stored
        // in blocks of size (page size)
        // CHANGING THIS VALUE IS NOT RECOMENDED
        def merges_compactionFactor = propFactory.getIntProperty("merges.compactionFactor",2).get()
        // min size of merge, larger value means merge will happen later,
        // but will affect performance of other tasks
        // value should be large enough to benefit sequential read/write performance
        // merges are expected to be quite small
        // all merges are performed on separate thread
        // NOT IN USE: for large merges there is multi-step implementation, every step is of this size
        def merges_minSize = propFactory.getIntProperty("merges.minSize", 256 * KB).get()
        // max size of merge
        // max size of merge doesn't mean it will be max size of a single transaction commit
        // because the later may contain many merges
        // single commit size is not restricted for now
        // same size for both means that merge will be triggered after reaching the threshold
        def merges_MaxSize = propFactory.getIntProperty("merges.minSize", 4 * MB).get()
    }
}
