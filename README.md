Tuapait
======

A simple local cache built on top of Berkely DB Java edition.

TCache is a cache for Serializable objects with a String key.
It uses <a href="http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html">Berkeley DB Java edition</a> for strage.
It uses the <a href="http://code.google.com/p/kryo/">Kryo serializer<a> so serializing/deserializing is both fast and small.

When an item is added to the cache, a timestamp is also recorded.

Cache Eviction: The default (and at present only avvailable) eviction strategy is an evict-on-read eviction strategy. This means that there is no active eviction of objects.
This means that if items are only written, they will not be evicted (and may get too large).

TCache can be used as a key-value store (NOSQL), if the eviction time is set to <a href="http://docs.oracle.com/javase/6/docs/api/java/lang/Long.html#MAX_VALUE">Long.MAX_VALUE</a> minutes.

###Usage###
Example:
(Coming...)

```
Example.java
```


###Name###
Named after <a href="https://en.wikipedia.org/wiki/Tuapait_Island">Tuapait Island</a>, Nunavut, Canada: 

