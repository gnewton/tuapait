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
    public static final String KEY_ENCODING_KEY= "keyEncoding.BDBCache";

    public static final String DEFAULT_CACHE_NAME = "bdbcache";

    private long timeToLiveMinutes = 60; // not Long.MAX_VALUE;
    //private long timeToLiveMinutes = Long.MAX_VALUE;

    private static final Lock lock = new ReentrantLock();
    private static final Logger LOGGER = Logger.getLogger(BDBCache.class.getName()); 
    private static final Map<String, Database>openDatabases = new HashMap<String, Database>();
    private static String keyEncoding="UTF-8";
    private static int transactionSize = 32;

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


    public void init(Properties p) {

	handleProperties(p);

	try{
	    init(dbDir, overWrite, readOnly);
	}catch(Throwable t){
	    t.printStackTrace();
	    if(brokenConfigReason == null){
		brokenConfigReason = "Generic init failure [" + t.getMessage() + "]";
	    }
	    LOGGER.warning(brokenConfigReason);
	    brokenConfig = true;
	}
    }


    public void delete(String key){
	LOGGER.info("START delete: " + key);

	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality; unable to delete()");
	    return;
	}

	lock.lock();
	try{	    
	    if(!inited){
		brokenConfigReason = "Config broken: did not run init(); key=" + key;
		LOGGER.warning(brokenConfigReason);
		brokenConfig = true;
		return;
	    }

	    DatabaseEntry deKey = new DatabaseEntry(key.getBytes(keyEncoding));
	    db.delete(transaction, deKey);
	    ++transactionCount;
	}
	catch(Exception e){
	    e.printStackTrace();
	    return;
	}
	finally{
	    lock.unlock();
	    LOGGER.info("End delete: " + key);
	}
	
    }


    public void put(String key, Serializable rec){
	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality; unable to put()");
	    return;
	}

	if(db == null){
	    LOGGER.warning("DB is null: did you init?");
	}
	if(rec == null){
	    LOGGER.warning("Trying to insert null object");
	    return;
	}

	try{
	    Carrier carrier = new Carrier();
	    carrier.object = rec;
	    byte[] bytes = Serializer.serializeRecord(carrier);

	    if(bytes == null){
		return;
	    }
	    DatabaseEntry deKey = new DatabaseEntry(key.getBytes(keyEncoding));
	    OperationStatus status = null;
	    DatabaseEntry deData = new DatabaseEntry(bytes);
	    lock.lock();
	    try{
		if(!inited){
		    brokenConfigReason = "Config broken: did not run init() in put();  key=" + key;
		    LOGGER.warning(brokenConfigReason);
		    brokenConfig = true;
		    return;
		}
		if(db == null){
		    LOGGER.info("BBBBBBBBBBBBBBBBBBBBBBBBB db is null; tsn=" + key);
		    return;
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
	    e.printStackTrace();
	}
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
	    
	    Carrier c = getCarrier(key);
	    if(c == null || c.object == null){
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
		return null;
	    }
	    
	    Carrier carrier = getCarrier(key);
	    if(carrier == null){
		return null;
	    }
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



    public void close(){
	LOGGER.info("Cache hits: " + hits + " misses=" + misses + " %hits=" + ((hits+misses==0)?"NA":(float)hits / (float)(hits+misses)));
	LOGGER.info("CLOSING DATABASE.... numItems=" + size());
	LOGGER.info("Num open databases: " + openDatabases.size());
	
	lock.lock();
	try{
	    brokenConfig = false;
	    inited = false;
	    LOGGER.info("Transaction=" + transaction);
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
	}
	finally{
	    lock.unlock();
	}
	LOGGER.info("DATABASE SUCCESSFULLY CLOSED...." );
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


    public void clear(){
	if(brokenConfig){
	    LOGGER.warning("Config broken: no caching functionality");
	    return;
	}

	close();
	lock.lock();
	try{
	    db = null;
	    init(dbDir, true, readOnly);
	}catch(Throwable t){
	    t.printStackTrace();
	}
	finally{
	    lock.unlock();
	}
    }



    //////// private ///////////////////////////////////////////////////

    protected void init(final String dbDir, final boolean overWrite, final boolean readOnly) throws DatabaseException{
	LOGGER.info("OPENING DATABASE...." + dbDir);

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

		}
		catch(SecurityException e){
		    e.printStackTrace();
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
		env.setup(f, false);
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


    private Carrier getCarrier(String key){
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
	    if(db == null){
		init(null);
	    }
	    DatabaseEntry deData = new DatabaseEntry();
	    deData.setPartial(0, 0, true);
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
		Carrier carrier = (Carrier) Serializer.deserializeRecord(deData.getData(), Carrier.class);
		return carrier;
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
	File[]files = f.listFiles();
	for(File file: files){
	    
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
	long nowMinutes = System.currentTimeMillis()/1000/60;
	boolean evict = (nowMinutes -timeStampMinutes) > timeToLiveMinutes;
	if(evict){
	    LOGGER.info("Evicting: " + key);
	}
	return evict;
    }
}



