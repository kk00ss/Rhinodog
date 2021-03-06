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

import com.codahale.metrics.MetricRegistry
import com.netflix.config.DynamicPropertyFactory
import rhinodog.Core.Definitions.BaseTraits._

import scala.collection.mutable

object Configuration {
    // Constants
    val KB = 1024
    val MB = 1024 * KB
    val GB = 1024l * MB

    object storageModeEnum
        extends Enumeration {
        type storageMode = this.Value
        val READ_ONLY, READ_WRITE, CREATE = this.Value
    }

    case class MainComponents
    (measureSerializer: MeasureSerializerBase,
     repository: IRepository,
     metadataManager: IMetadataManager,
     metrics: MetricRegistry)

    case class TermsDocsHashConfig
    (mainComponents: MainComponents,
     targetSize: Int)

    object GlobalConfig {
        val propFactory = DynamicPropertyFactory.getInstance()
        // enables flushing timer,
        // otherwise only manual flushing or closing an index will commit changes
        def storage_autoFlush = propFactory.getBooleanProperty("storage.autoFlush", true).get()
        // interval in seconds
        // converted to milliseconds
        def storage_flushInterval = propFactory.getLongProperty("storage.flushInterval", 30).get() * 1000
        // time to wait for running queries to complete before closing the index
        def storage_waitOnClose = propFactory.getLongProperty("storage.waitOnClose", 1000).get()
        // set to number of cores on your machine,
        // Sets the number of termsDocsHashes that will be created and
        // number of threads that will be used for encoding of postings at flush
        // larger number speeds up flushing process
        def global_numCores = propFactory.getIntProperty("global.numCores", 4).get()
        // influences disk IO granularity, 4KB - better caching, larger blocks -
        // (at the size of your SSD block) - faster IO
        def storage_pageSize = propFactory.getIntProperty("storage.pageSize", 4*KB).get()
        def storage_targetBlockSize: Int = storage_pageSize - 16 // should always be pageSize - 16
        //LMDB folder will be path + '\'+"InvertedIndex"
        def storage_path = propFactory.getStringProperty("storage.path","storageFolder").get()
        //storage space is acquired in chunks of this size
        def storage_sizeIncreaseStep = propFactory.getLongProperty("storage.sizeIncreaseStep", 1*GB)
        // number of docIDs for which there will be single bitmap segment
        // smaller value means less efficient encoding, but less data to write on change
        def storage_bitSetSegmentRange = propFactory
            .getIntProperty("storage.bitSetSegmentRange", Short.MaxValue).get()
        //delay in ms - we need it to decouple compactions from analysis threads
        def merges_queueCheckInterval = propFactory.getLongProperty("merges.queueCheckInterval", 100)
        //async compactions will not start if the system CPU load is more than this
        def merges_cpuLoadThreshold = propFactory.getFloatProperty("merges.cpuLoadThreshold", 0.95f)
        // compactions are only performed to reclaim space used by deleted documents
        // there is no need for further consolidation of data if it is already stored
        // in blocks of size (page size)
        // CHANGING THIS VALUE IS NOT RECOMENDED
        def merges_compactionFactor = propFactory.getIntProperty("merges.compactionFactor",2)
        // min size of merge, larger value means merge will happen later,
        // but will affect performance of other tasks
        // value should be large enough to benefit sequential read/write performance
        // merges are expected to be quite small
        // all merges are performed on separate thread
        // NOT IN USE: for large merges there is multi-step implementation, every step is of this size
        def merges_minSize = propFactory.getIntProperty("merges.minSize", 256 * KB)
        // max size of merge
        // max size of merge doesn't mean it will be max size of a single transaction commit
        // because the later may contain many merges
        // single commit size is not restricted for now
        // same size for both means that merge will be triggered after reaching the threshold
        def merges_maxSize = propFactory.getIntProperty("merges.maxSize", 4 * MB)
        // ==== MONITORING
        //Enable Slf4jReporter reporter for metrics
        def metrics_slf4j = propFactory.getBooleanProperty("metrics.slf4j", true).get()
        //Enable JMXReporter reporter for metrics
        def metrics_jmx = propFactory.getBooleanProperty("metrics.jmx", true).get()
        // True = seconds - false = Minutes
        def metrics_rates = propFactory.getBooleanProperty("metrics.rates", true).get()
        // Reporting Interval in seconds
        def metrics_reportingInterval = propFactory
            .getIntProperty("metrics.reportingInterval", 60).get()

    }
}
