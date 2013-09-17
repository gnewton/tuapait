Tuapait
======

A simple local cache built on top of Berkeley DB Java edition.

TCache is a cache for Serializable objects with a String key.
It uses <a href="http://www.oracle.com/technetwork/database/berkeleydb/overview/index-093405.html">Berkeley DB Java edition</a> for storage.
It uses the <a href="http://code.google.com/p/kryo/">Kryo serializer<a> so serializing/deserializing is both fast and small.

When an item is added to the cache, a timestamp is also recorded.

Cache Eviction: The default (and at present only available) eviction strategy is an evict-on-read eviction strategy. This means that there is no active eviction of objects.
This means that if items are only written, they will not be evicted (and may get too large).

TCache can be used as a key-value store (NOSQL), if the eviction time is set to <a href="http://docs.oracle.com/javase/6/docs/api/java/lang/Long.html#MAX_VALUE">Long.MAX_VALUE</a> minutes.

###Usage###
Example: ca/gnewton/tuapait/example/Example.java

```
public class Example{
    static private int n = 1000000;
    static private int nDel;
    static private int nPrint;
    static private String content = "This is some content";

    public static final void main(final String[] args) throws Exception {
	LogManager.getLogManager().reset();
	Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.OFF);

	if(args.length > 0){
	    n = Integer.decode(args[0]);
	}
	nDel = n/10;
	nPrint = n/100;

	// default cache location is "java.io.tmpdir"/bdbcache; on Linux this is usually "/tmp/bdbcache"
	TCache cache = TCacheManager.instance(null);

	// Add key/values to cache
	System.out.println("Adding " + n + " items to cache");
	for(int i=0; i<n; i++){
	    String key = "key_" + i;
	    String value = content + i;
	    cache.put(key, value);
	    if(i%nPrint == 0){
		System.out.println(i);
	    }
	}
	System.out.println("Number of items in cache: " + cache.size());
	System.out.println("Cache size: " + cache.storageSize() + "MB");

	
	// Get key/values from cache	
	System.out.println("Getting all values from cache");
	for(int i=0; i<n; i++){
	    String key = "key_" + i;
	    if(cache.containsKey(key)){
		String value = (String)cache.get(key);
		if(!value.equals(content + i)){
		    throw new Exception("Content is incorrect");
		}
	    }else{
		System.out.println("No value for key: " + key + ";  This should not happen");
	    }
	    if(i%nPrint == 0){
		System.out.println(i);
	    }
	}
	System.out.println("Number of items in cache: " + cache.size());
	System.out.println("Cache size: " + cache.storageSize() + "MB");

	// Randomly delete from cache
	System.out.println("Randomly deleting " + nDel + " items from cache");
	Random rand = new Random();
	for(int i=0; i<nDel; i++){
	    String key = "key_zz";
	    while(!cache.containsKey(key)){
		int randDel = rand.nextInt(n);
		key = "key_" + randDel;
		}
	    cache.delete(key);
	}    
	System.out.println("Number of items in cache: " + cache.size());
	System.out.println("Cache size: " + cache.storageSize() + "MB");


	// Empty cache
	System.out.println("Clearing cache");
	cache.clear();
	System.out.println("Number of items in cache: " + cache.size());
	System.out.println("Cache size: " + cache.storageSize() + "MB");

	cache.close();
    }    
}

```


###Name###
Named after <a href="https://en.wikipedia.org/wiki/Tuapait_Island">Tuapait Island</a>, Nunavut, Canada: 

