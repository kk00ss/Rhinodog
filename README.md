# Rhinodog
Implementation of inverted index on top of LMDB with some interesting optimizations. 
Written for fun, work in progress. (Rhinodog - from russian for strange creature "Смесь бульдога с носорогом")
For understanding why this project was created see Main features section
Implementation of inverted index on top of LMDB with some interesting optimizations. Written for fun, work in progress.

##Current state
* readiness - Limited features, not 100% tests coverage. 
* Unit and regression tests - work in progress. 
* Indexing performance -  on enwiki texs of size 569MB it takes 50sec with Core i7 4790K and SSD Samsung 840 EVO, which is about 40GB/Hour, which is 50% of Lucene's indexing performance on my hardware. Lucene's English analyzer is used, and similar approach in general, but updating B-tree is quite slow (write speed in TaskManager are much smaller) and Rhinodog uses ConcurrentSkipList for TermHash which is slower than Lucene's TermHash. ConcurrentSkipList - will help in developing better NRT (Near Real Time) search. 
* Performance tests - were run on generated documents and real tests. Results vary from 2-3 times slower than Lucene to same 2-3 times faster than Lucene. This difference depend on how do query terms correlate with each other, if they are common words that occur in almost every document - Lucene is faster, the larger the difference in term frequencies of query terms - the better it is for Rhinodog. Benchmarking code in main.scala describes the situation when query terms are very correlated.  Educated guess would be - because Red-Black-Tree is faster than skip-lists. But it's hard to say for sure.

##Plans
* Multifield documents support
* Faceting
 
##Small example

```scala
        val invertedIndex = new InvertedIndex(new OkapiBM25MeasureSerializer(),
                                              new EnglishAnalyzer(),
                                              storageModeEnum.CREATE)
        val document = scala.io.Source.fromFile("testArticle.txt").mkString
        val ID = invertedIndex.addDocument(Document(document))
        invertedIndex.flush()
        // 1 - is a termID, automatic conversion of word -> stem -> termID, will be added soon
        val topLevelIterator = invertedIndex
            .getQueryEngine()
            .buildTopLevelIterator(ElementaryClause("mississippi")) //English analyzer lowercases every word

        val ret = invertedIndex.getQueryEngine().executeQuery(topLevelIterator, 10)
```

##Main features:
*  Use LMDB transaction to maintain consistant state of an index. So it will be hard to break it, or lose data.
*  Compactions are performed on per term basis, so each of those will be much smaller (a few MBs not GBs) -
*  Rare/Smaller compactions - as far as I understand Lucene performs compactions in order to avoid random reads. Which are slow on HDDs. But today we have cheap SSDs so random IO is cheaper, and it makes sense to merge smaller blocks on per term basis, only when it is absolutly necessary. (That would cause random IO and cause horrible performance on HDD). Current implementation uses $pageSize = 4KB as max size for a block - as to benefit from OS virtual memory (but it might be better to increase it to 64KB for better random IO performance). There are only 2 cases when compaction will be performed A) Merge small blocks of uneven sizes into $pageSize blocks in a single multi-MB compaction B) Merge a lot of $pageSize blocks which are at least half empty, if their combined size is larger than minCompactionSize
*  Use block's and segment's metadata to avoid decoding blocks and segments that cannot give TOP K matches.
*  Flexible storage format and pluggable analysis. Current version contains examples of using Lucene's analyzers and Okapi BM25 scoring. Analysis and Measure format are closely coupled in order to avoid explosion of abstractions.
*  It's relatively easy to implement scoring at indexing time (BM25 without IDF can be computed and stored at indexing time, with assumption that we know avarage document size in advance)

##List of opensource software used:
*   [Lightning Memory mapped Data Base -LMDB](https://github.com/LMDB/lmdb)  
*   [Deephaks LMDB driver](https://github.com/deephacks/lmdbjni)
*   [Roaring bitmaps](https://github.com/RoaringBitmap/RoaringBitmap)
*   [JavaFastPFor](https://github.com/lemire/JavaFastPFOR)
*   [Netflix archaius](https://github.com/Netflix/archaius)
*   [Metrics from Dropwizard](http://metrics.dropwizard.io/)
*   [SLF4J](http://www.slf4j.org/)
*   [Logback](http://logback.qos.ch/)	
*   [Netflix archaius](https://github.com/Netflix/archaius)
*   [Junit](https://github.com/junit-team)
*   [Lucene 6.0.0 analyzers and for comparison](https://github.com/apache/lucene-solr)
