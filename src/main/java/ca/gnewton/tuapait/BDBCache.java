package ca.gnewton.tuapait;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import com.sleepycat.je.EnvironmentFailureException;

public class BDBCache implements TCache
{
    public static final String DB_DIR_KEY= "dbdir.BDBCache";
    public static final String OVERWRITE_KEY = "overwrite.BDBCache";
    public static final String READ_ONLY_KEY= "readOnly.BDBCache";
    public static final String TTL_MINUTES_KEY= "timeToLiveMinutes.BDBCache";
    public static final String KEY_ENCODING_KEY = "keyEncoding.BDBCache";

    public static final String DEFAULT_CACHE_NAME = "bdbcache";

    public static final String BDB_DB_NAME = "ca.gnewton.tuapait.BDBCache";

    private long timeToLiveMinutes = 60; // not Long.MAX_VALUE;
    //private long timeToLiveMinutes = Long.MAX_VALUE;

    private static final Lock lock = new ReentrantLock();
    private static final Logger LOGGER = Logger.getLogger(BDBCache.class.getName()); 
    private static final Map<String, Database>openDatabases = new HashMap<String, Database>();
    private String keyEncoding="UTF-8";
    private int transactionSize = 32;

    private volatile Database db = null;
    private volatile DbEnv env;
    private volatile String dbDir = System.getProperty("java.io.tmpdir") + File.separator +  DEFAULT_CACHE_NAME;
    private volatile String dbDirFq = null;
    private volatile String dbKey = null;
    private volatile Transaction transaction = null;
    private volatile boolean brokenConfig = false;
    private volatile String brokenConfigReason = null;
    private volatile boolean inited = false;
    private volatile boolean readOnly = false;

    private volatile boolean overWrite = false;
    private volatile int transactionCount = 0;
    private volatile long hits = 0;
    private volatile long misses = 0; 

    private volatile boolean lastGetAHit = false;

    protected BDBCache(){
	
    }

    public void init(Properties p) {
	handleProperties(p);
	try{
	    init(dbDir, readOnly);
	}catch(Throwable t){
	    t.printStackTrace();
	    if(brokenConfigReason == null){
		brokenConfigReason = "Generic init failure [" + t.getMessage() + "]";
	    }
	    LOGGER.warning(brokenConfigReason);
	    brokenConfig = true;
	}
    }


    public boolean delete(String key){
	LOGGER.info("START delete: " + key);

	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality; unable to delete()");
	    return false;
	}

	lock.lock();
	try{	    
	    if(!inited){
		brokenConfigReason = "Config broken: did not run init(); key=" + key;
		LOGGER.warning(brokenConfigReason);
		brokenConfig = true;
		return false;
	    }

	    DatabaseEntry deKey = new DatabaseEntry(key.getBytes(keyEncoding));
	    db.delete(transaction, deKey);
	    ++transactionCount;
	}
	catch(Exception e){
	    e.printStackTrace();
	    return false;
	}
	finally{
	    lock.unlock();
	    LOGGER.info("End delete: " + key);
	}
	return true;
    }


    public boolean put(String key, Serializable rec){
	//if(readOnly){
	//LOGGER.warning("Unable to write to READ-ONLY database; key=" + key);
	//return false;
	//}
	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality; unable to put()");
	    return false;
	}

	if(db == null){
	    LOGGER.warning("DB is null: did you init?");
	}
	if(rec == null){
	    LOGGER.warning("Trying to insert null object");
	    return false;
	}

	try{
	    Carrier carrier = new Carrier();
	    carrier.object = rec;
	    byte[] bytes = Serializer.serializeRecord(carrier);

	    if(bytes == null){
		return false;
	    }
	    DatabaseEntry deKey = null;
	    try{
		deKey = new DatabaseEntry(key.getBytes(keyEncoding));
	    }catch(java.io.UnsupportedEncodingException e){
		e.printStackTrace();
		return false;
	    }
	    OperationStatus status = null;
	    DatabaseEntry deData = new DatabaseEntry(bytes);
	    lock.lock();
	    try{
		if(!inited){
		    brokenConfigReason = "Config broken: did not run init() in put();  key=" + key;
		    LOGGER.warning(brokenConfigReason);
		    brokenConfig = true;
		    return false;
		}
		if(db == null){
		    LOGGER.info("BBBBBBBBBBBBBBBBBBBBBBBBB db is null; tsn=" + key);
		    return false;
		}
		if(transaction == null || transactionCount > transactionSize){
		    if(transaction != null && transaction.isValid()){
			LOGGER.info("Commiting transaction: " + transaction.getId() + " state=" + transaction.getState());
			transaction.commitSync();
			LOGGER.info("Committed transaction: " + transaction.getId() + " state=" + transaction.getState());
			LOGGER.info("Cache hits: " + hits + " misses=" + misses + " %hits=" + (float)hits / (float)(hits+misses));
		    }
		    TransactionConfig config = new TransactionConfig();
		    config.setReadUncommitted(true);
		    transaction = db.getEnvironment().beginTransaction(null, config);
		    LOGGER.info("Starting transaction: " + transaction.getId() + " state=" + transaction.getState());
		    transactionCount = 0;
		}
		LOGGER.info("Putting to transaction: " + transaction.getId() + " state=" + transaction.getState());
		status = db.put(transaction, deKey, deData);
		LOGGER.info("Put to transaction: " + transaction.getId() + " state=" + transaction.getState());
		++transactionCount;
		LOGGER.info("PUT: " + status + " key=" + key);
	    }
	    finally{
		lock.unlock();
	    }
	    //LOGGER.info("Put into cache tsn: " + rec.getTsn() + "  status: " + status);
	} catch (Throwable e) {
	    LOGGER.warning(e.getMessage());
	    //e.printStackTrace();
	    return false;
	}
	return true;
    }


    public boolean containsKey(String key){
	LOGGER.info("containsKey inited=" + inited);
	try{
	    if(!inited){
		brokenConfigReason = "Config broken: did not run init() in containsKey();  key=" + key;
		LOGGER.warning(brokenConfigReason);
		brokenConfig = true;
		return false;
	    }
	    if(brokenConfig){
		LOGGER.warning("Config broken: caching functionality turned off");
		return false;
	    }
	    
	    LOGGER.info("START containsKey: " + key);
	    
	    if(db == null){
		brokenConfigReason = "DB is null in containsKey(); key=" + key;
		LOGGER.warning(brokenConfigReason);
		brokenConfig = true;
		return false;
	    }
	    
	    Carrier c = getCarrier(key, true);
	    if(c == null){
		++misses;
		return false;
	    }

	    if(evictionTime(key, c.timeStampMinutes)){
		delete(key);
		return false;
	    }
	    ++hits;
	    return true;
	}
	catch(Throwable t){
	    t.printStackTrace();
	    return false;
	}
	finally{
	    LOGGER.info("END containsKey: " + key);
	}

    }


    public Object get(String key){
	try{
	    if(!inited){
		brokenConfigReason = "Config broken: did not run init(); containsKey(); key=" + key;
		LOGGER.warning(brokenConfigReason);
		brokenConfig = true;
		lastGetAHit = false;
		return null;
	    }
	    
	    Carrier carrier = getCarrier(key, false);
	    if(carrier == null){
		lastGetAHit = false;
		return null;
	    }
	    lastGetAHit = true;
	    return carrier.object;
	}
	catch(Throwable t){
	    t.printStackTrace();
	    return null;
	}
	finally{
	    LOGGER.info("END get: " + key);
	}
    }



    public boolean close(){
	LOGGER.info("Cache hits: " + hits + " misses=" + misses + " %hits=" + ((hits+misses==0)?"NA":(float)hits / (float)(hits+misses)));
	LOGGER.info("CLOSING DATABASE.... numItems=" + size());
	LOGGER.info("Num open databases: " + openDatabases.size());
	
	lock.lock();
	try{
	    brokenConfig = false;
	    inited = false;
	    LOGGER.info("Transaction=" + transaction);
	    if(readOnly && transaction != null){
		transaction.abort();
	    }
	    if(transaction != null){
		LOGGER.info("Outstanding transaction: " + transaction.getId() + " state=" + transaction.getState() + " db=" + db);
		if(transaction.isValid()){
		    LOGGER.info("Committing transaction: " + transaction.getId() + " state=" + transaction.getState() + " db=" + db);
		    transaction.commitSync();
		}
		transaction = null;
	    }

	    if(db != null){
		db.close();
		db = null;
	    }

	    openDatabases.remove(dbKey);
	    if(env != null && env.getEnv() != null){
		env.getEnv().close();
	    }
	}
	catch(Throwable t){
	    LOGGER.severe("DATABASE CLOSE FAIL....");
	    t.printStackTrace();
	    return false;
	}
	finally{
	    lock.unlock();
	}
	LOGGER.info("DATABASE SUCCESSFULLY CLOSED...." );
	return true;
    }


    public long size(){
	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality");
	    return 0;
	}
	if(db == null){
	    return 0;
	}
	try{
	    return db.count();
	}catch(Throwable t){
	    t.printStackTrace();
	    return 0;
	}
    }


    public boolean clear(){
	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality");
	    return false;
	}

	close();
	lock.lock();
	try{
	    db = null;
	    overWrite = true;
	    init(dbDir, readOnly);
	}catch(Throwable t){
	    t.printStackTrace();
	    return false;
	}
	finally{
	    lock.unlock();
	}
	return true;
    }



    //////// private ///////////////////////////////////////////////////

    protected void init(final String dbDir, final boolean readOnly) throws DatabaseException{
	LOGGER.info("OPENING DATABASE....[" + dbDir + "] overWrite=" + overWrite + "  readOnly=" + readOnly + " encoding=" + keyEncoding + " TTL minutes=" + timeToLiveMinutes);

	if(brokenConfig){
	    LOGGER.info("OPENING DATABASE....broken config: " + brokenConfigReason);
	    return;
	}

	lock.lock();
	try{
	    this.dbDir = dbDir;
	    this.readOnly = readOnly;
	    this.dbKey = dbDir + readOnly;

	    inited = true;
	    if(openDatabases.containsKey(dbKey)){
		db = openDatabases.get(dbKey);
		if(db != null){
		    LOGGER.info("OPENING DATABASE....EXISTING: " + db);
		    return;
		}
	    }
	    if(db != null){
		LOGGER.info("ALREADY OPENED DATABASE...." + dbDir);
		return;
	    }else{
		env = new DbEnv();
		File f = new File(dbDir);
		try{
		    dbDirFq = f.getCanonicalPath();
		}catch(IOException ie){
		    ie.printStackTrace();
		    brokenConfigReason = "Unable to create db directory: IOException: " + ie.getMessage();
		    LOGGER.warning(brokenConfigReason);
		    brokenConfig = true;
		    return;
		}
		catch(SecurityException e){
		    e.printStackTrace();
		    brokenConfigReason = "Unable to create db directory: SecurityException: " + e.getMessage();
		    LOGGER.warning(brokenConfigReason);
		    brokenConfig = true;
		}
		if(f != null && f.exists()){
		    if(overWrite){
			try{
			    deleteDbDirectoryContents(f);
			}
			catch(Throwable t){
			    brokenConfigReason = "Unable to create db directory: " + f.getName() 
				+ " [" + t.getMessage() + "]";
			    LOGGER.warning(brokenConfigReason);
			    brokenConfig = true;
			    return;
			}
		    }
		}else{
		    boolean created;
		    try{
			created = createDbDirectory(f);
		    }
		    catch(Throwable t){
			brokenConfigReason = "Unable to create db directory: " + f.getName() 
			    + " [" + t.getMessage() + "]";
			LOGGER.warning(brokenConfigReason);
			brokenConfig = true;
			return;
		    }
		    if(!created){
			brokenConfigReason = "Unable to create db directory: " + f.getName();
			LOGGER.warning(brokenConfigReason);
			brokenConfig = true;
			return;
		    }
		}
		env.setup(f, BDB_DB_NAME, readOnly, true);
		db = env.getDB();
		openDatabases.put(dbKey, db);

		LOGGER.info("SUCCESSFULY OPENED DATABASE: " + dbDirFq + "  db=" + db);
	    }
	}
	catch(EnvironmentFailureException e){
	    e.printStackTrace();
	    inited = true;
	    brokenConfig = true;
	    brokenConfigReason = "FAILED OPENING DATABASE: " + dbDirFq + "  db=" + db + " [" + e.getMessage() + "]";
	    LOGGER.warning(brokenConfigReason);
	    throw e;
	}
	catch(DatabaseException e){
	    e.printStackTrace();
	    inited = true;
	    brokenConfig = true;
	    brokenConfigReason = "FAILED OPENING DATABASE: " + dbDirFq + "  db=" + db + " [" + e.getMessage() + "]";
	    LOGGER.warning(brokenConfigReason);
	    throw e;
	}
	finally{
	    lock.unlock();
	}
    }


    private Carrier getCarrier(String key, boolean keyOnly){
	LOGGER.info("START getCarrier: " + key);
	try{
	    if(brokenConfig){
		LOGGER.warning("Config broken; " + brokenConfigReason);
		return null;
	    }
	    
	    if(db == null){
		brokenConfigReason = "Config broken: DB is null; getCarrier; key=" + key;
		LOGGER.warning(brokenConfigReason);
		brokenConfig = true;
		return null;
	    }
	    
	    if(key == null){
		return null;
	    }

	    DatabaseEntry deData = new DatabaseEntry();

	    OperationStatus status = null;
	    try{
		DatabaseEntry deKey = new DatabaseEntry(key.getBytes(keyEncoding));

		try{ // db might still be null as we do not lock for get's; db may close after the above check...//
		    status = db.get(null, deKey, deData, com.sleepycat.je.LockMode.READ_UNCOMMITTED);
		}
		catch(Throwable t){
		    return null;
		}

		if(status == OperationStatus.NOTFOUND){
		    LOGGER.info("MISS: " + key);
		    return null;
		}
	    }catch (Exception e) {
		e.printStackTrace();
		return null;
	    }
	    
	    LOGGER.info("HIT: " + key);
	    try{
		return (Carrier) Serializer.deserializeRecord(deData.getData(), Carrier.class);
	    }catch(Throwable t){
		t.printStackTrace();
		return null;
	    }
	}finally{
	    LOGGER.info("END getCarrier: " + key);
	}
    }


    private void deleteDbDirectoryContents(File f) throws IOException{
	if(f == null){
	    return;
	}
	if(!f.exists()){
	    throw new IOException("Does not exist: [" + f.getCanonicalPath() + "]");
	}
	LOGGER.info("Deleting contents of directory: " + f.getCanonicalPath());
	File[]files = f.listFiles();
	for(File file: files){
	    file.delete();
	}
    }

    private void handleProperties(final Properties p){
	if(p != null){
	    if(p.containsKey(DB_DIR_KEY)){
		dbDir = p.getProperty(DB_DIR_KEY);
	    }
	    if(p.containsKey(OVERWRITE_KEY)){
		overWrite = Boolean.parseBoolean(p.getProperty(OVERWRITE_KEY));
	    }
	    if(p.containsKey(READ_ONLY_KEY)){
		readOnly = Boolean.parseBoolean(p.getProperty(READ_ONLY_KEY));
	    }
	    if(p.containsKey(TTL_MINUTES_KEY)){
		timeToLiveMinutes = Long.decode(p.getProperty(TTL_MINUTES_KEY));
	    }
	    if(p.containsKey(KEY_ENCODING_KEY)){
		keyEncoding = p.getProperty(KEY_ENCODING_KEY);
	    }
	}
    }


    private boolean createDbDirectory(final File f) throws SecurityException{
	return f.mkdirs();
    }


    private boolean evictionTime(final String key, final long timeStampMinutes){
	boolean evict = false;

	if(timeToLiveMinutes == 0l){
	    evict = true;
	}else{
	    long nowMinutes = System.currentTimeMillis()/1000/60;
	    evict = (nowMinutes -timeStampMinutes) > timeToLiveMinutes;
	}

	if(evict){
	    LOGGER.info("Evicting key: " + key);
	}
	return evict;
    }

    protected boolean lastGetAHit(){
	return lastGetAHit;
    }

}



