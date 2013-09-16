package ca.gnewton.tuapait;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;


public class DbEnv {
    private final static Logger LOGGER = Logger.getLogger(DbEnv.class.getName()); 
   private Environment myEnv;

    private Database db;
    private Database classCatalogDb;
    // Needed for object serialization
    private StoredClassCatalog classCatalog;

    // Our constructor does nothing
    public DbEnv() {}

    // The setup() method opens all our databases and the environment
    // for us.
    public void setup(File envHome, String dbName, boolean readOnly, boolean allowCreate, long bdbLogFileSizeMb)
        throws DatabaseException {
	long logFileMax = 1024l * 1024l * bdbLogFileSizeMb;

        EnvironmentConfig myEnvConfig = new EnvironmentConfig();
	myEnvConfig.setConfigParam(EnvironmentConfig.CLEANER_EXPUNGE, "true");
	//myEnvConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_HIGH_PRIORITY, "true");
	//myEnvConfig.setConfigParam(EnvironmentConfig.CLEANER_BACKGROUND_PROACTIVE_MIGRATION, "true");
	//myEnvConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, "1");

        DatabaseConfig myDbConfig = new DatabaseConfig();

	myEnvConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, Long.toString(logFileMax));
	myEnvConfig.setConfigParam(EnvironmentConfig.LOCK_N_LOCK_TABLES, "7");

        myDbConfig.setReadOnly(readOnly);
	myEnvConfig.setAllowCreate(allowCreate);
	myDbConfig.setAllowCreate(allowCreate);

	//myDbConfig.setDeferredWrite(true);
        // Allow transactions if we are writing to the database
        //myEnvConfig.setTransactional(true);
	myEnvConfig.setTransactional(false);
        //myDbConfig.setTransactional(true);
        myDbConfig.setTransactional(false);
	myDbConfig.setDeferredWrite(true);
	try{
	    myEnv = new Environment(envHome, myEnvConfig);
	}catch(EnvironmentLockedException e){
	    e.printStackTrace();
	    LOGGER.severe("Unable to get lock for " + envHome + "; likely because another BDB process owns the lock");
	    if(myEnv != null){
		myEnv.close();
	    }
	    throw e;
	}
	db = myEnv.openDatabase(null, dbName, myDbConfig);
    }

   // getter methods

    // Needed for things like beginning transactions
    public Environment getEnv() {
        return myEnv;
    }

    public Database getDB() {
        return db;
    }


    public StoredClassCatalog getClassCatalog() {
        return classCatalog;
    }

    //Close the environment
    public void close() {
        if (myEnv != null) {
            try {
                //Close the secondary before closing the primaries
                db.close();
                //classCatalogDb.close();

                // Finally, close the environment.
                myEnv.close();
            } catch(DatabaseException dbe) {
                System.err.println("Error closing MyDbEnv: " +
                                    dbe.toString());
               System.exit(-1);
            }
        }
    }
}
