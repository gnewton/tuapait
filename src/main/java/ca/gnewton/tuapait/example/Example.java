package ca.gnewton.tuapait.example;

import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import ca.gnewton.tuapait.TCache;
import ca.gnewton.tuapait.TCacheManager;


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
