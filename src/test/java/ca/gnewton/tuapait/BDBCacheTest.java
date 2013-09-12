package ca.gnewton.tuapait;

import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
//import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentFailureException;
import ca.gnewton.tuapait.BDBCache;

@RunWith(JUnit4.class)
public class BDBCacheTest{


    //@Test(expected=java.io.IOException.class)
    //@Test(expected=EnvironmentFailureException.class)
    @Test
    public void initShouldFailWithImpossibleDir() throws EnvironmentFailureException{
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, "/");
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.READ_ONLY_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");
	    BDBCache c = new BDBCache();
	    c.init(p);
	}

	catch(EnvironmentFailureException efe){
	    Assert.assertTrue(true);
	}
	catch(Throwable t){
	    System.out.println("=====================");
	    t.printStackTrace();
	    System.out.println("=====================");
	    Assert.fail("This should not happen");
	    }
    }
}
