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
import com.sleepycat.je.EnvironmentFailureException;

public class BDBCache implements TCache
{
    private String keyEncoding="UTF-8";
    private int deferredWriteSize = 32;
    private long bdbLogFileSizeMb = 16l; // bdb defaults
    private long timeToLiveMinutes = 60*24*7*365; // not Long.MAX_VALUE;

    private static final Lock lock = new ReentrantLock();
    private static final Logger LOGGER = Logger.getLogger(BDBCache.class.getName()); 
    private static final Map<String, Database>openDatabases = new HashMap<String, Database>();
    private static final Map<String, Integer>openDatabaseClientCount = new HashMap<String, Integer>();
    private static final String BDB_DB_NAME = "ca.gnewton.tuapait.TCache";

    private volatile Database db = null;
    private volatile DbEnv env;
    private volatile String brokenConfigReason = null;
    private volatile String dbDir = System.getProperty("java.io.tmpdir") + File.separator +  DEFAULT_CACHE_NAME;
    private volatile String dbDirFq = null;
    private volatile String dbKey = null;
    private volatile boolean brokenConfig = false;
    private volatile boolean inited = false;
    private volatile boolean lastGetAHit = false;
    private volatile boolean overWrite = false;
    private volatile boolean readOnly = false;
    private volatile int deferredWriteRecordCount = 0;
    private volatile long hits = 0;
    private volatile long misses = 0; 

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
	if(readOnly){
	    LOGGER.info("No deletes: read only cache: " + key);
	    return false;
	}


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
	    db.delete(null, deKey);
	    ++deferredWriteRecordCount;
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
	if(readOnly){
	    LOGGER.warning("No puts: read only cache: " + key);
	    return false;
	}
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
	    byte[] dataBytes = makeDataBytes(rec);

	    if(dataBytes == null){
		return false;
	    }
	    DatabaseEntry deKey = null;
	    try{
		deKey = new DatabaseEntry(makeKeyBytes(key, keyEncoding));
	    }catch(java.io.UnsupportedEncodingException e){
		e.printStackTrace();
		return false;
	    }
	    OperationStatus status = null;
	    DatabaseEntry deData = new DatabaseEntry(dataBytes);
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
		if(deferredWriteRecordCount > deferredWriteSize){
		    sync();
		}

		status = db.put(null, deKey, deData);
		++deferredWriteRecordCount;
		LOGGER.info("PUT: " + status + " key=" + key);
	    }
	    finally{
		lock.unlock();
	    }
	} catch (Throwable e) {
	    LOGGER.warning(e.getMessage());
	    e.printStackTrace();
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

	    if(timeToEvict(key, c.timeStampMinutes)){
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
	LOGGER.info("CLOSING CACHE.... numItems=" + size());
	LOGGER.info("Num open databases: " + openDatabases.size());
	
	lock.lock();
	try{
	    int numClients = decrementOpenDatabaseClientCount(openDatabaseClientCount, dbKey);
		
	    if(numClients == 0){
		brokenConfig = false;
		inited = false;
		openDatabases.remove(dbKey);

		if(db != null){
		    db.close();
		    LOGGER.info("CLOSED: inner BDB database: " + db);
		    db = null;
		}

		
		if(env != null && env.getEnv() != null){
		    env.getEnv().close();
		}
	    }
	}
	catch(Throwable t){
	    LOGGER.severe("CACHE CLOSE FAIL....");
	    t.printStackTrace();
	    return false;
	}
	finally{
	    lock.unlock();
	}
	LOGGER.info("CACHE SUCCESSFULLY CLOSED...." );
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

    public long storageSize(){
	if(db == null){
	    return 0;
	}
	sync();
	File h = db.getEnvironment().getHome();

	if(!h.exists()){
	    LOGGER.warning("Database home dir does not exist: " + h.getName());
 	    return 0;
	}

	if(!h.canRead()){
	    LOGGER.warning("Unable to read database home dir: " + h.getName());
 	    return 0;
	}

	long size = 0l;
	File[] files = h.listFiles();
	for(File f: files){
	    if(f.isFile()){
		size += f.length();
	    }
	}
	return size/1000000;
    }

    //////// private ///////////////////////////////////////////////////

    protected void sync(){
	if(db == null){
	    return;
	}
	LOGGER.info("Syncing to db");
	db.sync();
	deferredWriteRecordCount = 0;
    }

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
		    incrementOpenDatabaseClientCount(openDatabaseClientCount, dbKey);
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
		env.setup(f, BDB_DB_NAME, readOnly, true, bdbLogFileSizeMb);
		db = env.getDB();
		openDatabases.put(dbKey, db);
		incrementOpenDatabaseClientCount(openDatabaseClientCount, dbKey);

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
		DatabaseEntry deKey = new DatabaseEntry(makeKeyBytes(key,keyEncoding));

		try{ // db might still be null as we do not lock for get's; db may close after the above check...//
		    status = db.get(null, deKey, deData, com.sleepycat.je.LockMode.READ_UNCOMMITTED);
		}
		catch(Throwable t){
		    return null;
		}

		if(status == OperationStatus.NOTFOUND){
		    LOGGER.info("GET MISS: " + key);
		    return null;
		}
	    }catch (Exception e) {
		e.printStackTrace();
		return null;
	    }
	    
	    LOGGER.info("GET HIT: " + key);
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
	    if(p.containsKey(BDB_LOG_FILE_SIZE_MB_KEY)){
		bdbLogFileSizeMb = Long.decode(p.getProperty(BDB_LOG_FILE_SIZE_MB_KEY));
	    }

	    if(p.containsKey(BDB_DEFERRED_WRITE_SIZE_KEY)){
		deferredWriteSize = Integer.decode(p.getProperty(BDB_DEFERRED_WRITE_SIZE_KEY));
	    }
	}
    }


    private boolean createDbDirectory(final File f) throws SecurityException{
	return f.mkdirs();
    }


    protected boolean timeToEvict(final String key, final long timeStampMinutes){
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


    // These next three methods should only be called if inside of a lock; like in init() and close();
    private int decrementOpenDatabaseClientCount(Map<String, Integer>map, String dbKey){
	return alterOpenDatabaseClientCount(map, dbKey, -1);
    }
    private int incrementOpenDatabaseClientCount(Map<String, Integer>map, String dbKey){
	return alterOpenDatabaseClientCount(map, dbKey, 1);
    }

    private int alterOpenDatabaseClientCount(Map<String, Integer>map, String dbKey, int amount){
	if(map == null){
	    return 0;
	}
	LOGGER.info("changing value: dbKey: " + dbKey + " " + amount);
	Integer count = null;
	if(map.containsKey(dbKey)){
	    count = new Integer(map.get(dbKey).intValue() + amount);
	}else{
	    count = new Integer(amount);
	}
	map.put(dbKey, count);
	LOGGER.info("NEW value: " + count);
	return count.intValue();
    }


    private byte[] makeDataBytes(Serializable rec){
	Carrier carrier = new Carrier();
	carrier.object = rec;
	return Serializer.serializeRecord(carrier);
    }

    private byte[] makeKeyBytes(String key, String encoding) throws java.io.UnsupportedEncodingException{
	return key.getBytes(keyEncoding);
    }
    
}



