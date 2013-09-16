package ca.gnewton.tuapait;

import java.io.Serializable;
import java.util.Properties;

public class TCacheManager{

    public static final TCache instance(Properties p){
	BDBCache cache = new BDBCache();
	cache.init(p);
	return cache;
    }

}
