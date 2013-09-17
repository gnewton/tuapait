package ca.gnewton.tuapait;

import java.io.Serializable;
import java.util.Properties;

public class TCacheManager{

    public static final TCache instance(Properties p){
	TCache cache = null;
	CacheType cacheType = null;

	if(p == null || !p.containsKey(TCache.CACHE_TYPE_KEY)) {
	    cacheType = CacheType.CACHE;
	}else{
	    String tmp = p.getProperty(TCache.CACHE_TYPE_KEY);
	    if(tmp.equals(CacheType.STORE.toString())){
		cacheType = CacheType.STORE;
	    }else if(tmp.equals(CacheType.CACHE.toString())){
		cacheType = CacheType.CACHE;
	    }else if(tmp.equals(CacheType.IMMUTABLE_STORE.toString())){
		cacheType = CacheType.IMMUTABLE_STORE;
	    }
	}
	System.out.println("----------------------------------------- cacheType=" + cacheType);
	switch (cacheType){
	case STORE:
	    cache = new BDBStore();
	    break;
	    
	case IMMUTABLE_STORE:
	    cache = new BDBImmutableStore();
	    break;

	case CACHE:
	default:
	    cache = new BDBCache();
	    break;
	}
	
	cache.init(p);
	return cache;
    }

}
