package ca.gnewton.tuapait.example;

import ca.gnewton.tuapait.TCache;
import ca.gnewton.tuapait.BDBCache;
import ca.gnewton.tuapait.TCacheManager;
import java.util.logging.Level;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.LogManager;


public class Simple extends Thread{
    String content = "abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz abcdefghijklmnopqrstuvwxyz";
    String prefix="";
    int n = 500000;

    public static final void main(final String[] args) {

	Simple simple = new Simple("");
	simple.start();
    }


    public Simple(final String prefix){
	this.prefix = prefix;
    }

    public void run(){
	LogManager.getLogManager().reset();
	Logger globalLogger = Logger.getLogger(java.util.logging.Logger.GLOBAL_LOGGER_NAME);
        globalLogger.setLevel(java.util.logging.Level.OFF);

	Properties p = new Properties();
	p.setProperty(TCache.DB_DIR_KEY, "/tmp/bdb_cache");
	p.setProperty(TCache.BDB_LOG_FILE_SIZE_MB, "256");
	TCache cache = TCacheManager.instance(p);
	String key = prefix + "hey";
	for(int i=0; i<n; i++){
	    //Long lvalue = new Long(19+i);
	    String lvalue = content + i;new String();
	    cache.put(key+i, lvalue);
	    if(i%100000 == 0){
		System.out.println(prefix + " " + i);
	    }
	    //System.out.println(key+i + ":" + cache.get(key+i));
	}
	cache.close();
    }

}
