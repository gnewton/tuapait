package ca.gnewton.tuapait;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import ca.gnewton.tuapait.BDBCache;
import com.sleepycat.je.EnvironmentFailureException;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BDBCacheTest{

    String key = "hello";
    String value = "world";
    String key2 = "hello999";
    String value2 = "foobar";

    @Test
    public void shouldFailWithImpossibleDir(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, "/shouldFailWithImpossibleDir");
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);
	    c.close();
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
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }



    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    String tmpDir(String s) throws IOException{
	return tmpDir.newFolder(s).getCanonicalPath();
    }

    @Test
    public void shouldOpenAndCloseInTempDirOK(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldOpenAndCloseInTempDirOK"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

    
    @Test
    public void shouldFailWritingToReadOnlyDB(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldFailWritingToReadOnlyDB"));
	    p.setProperty(BDBCache.READ_ONLY_KEY, "false");
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    System.out.println(p);
	    c = (BDBCache)TCacheManager.instance(p);
	    Assert.assertTrue(c.put(key, value));
	    c.close();

	    p.setProperty(BDBCache.READ_ONLY_KEY, "true");
	    c = (BDBCache)TCacheManager.instance(p);
	    Assert.assertFalse(c.put(key, value2));
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

    @Test
    public void shouldFailWithBadByteEncoding(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldFailWithBadByteEncoding"));
	    p.setProperty(BDBCache.READ_ONLY_KEY, "false");
	    p.setProperty(BDBCache.KEY_ENCODING_KEY, "foobar-887d3");
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");

	    c = (BDBCache)TCacheManager.instance(p);
	    Assert.assertFalse(c.put(key, value));


	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }


    @Test
    public void shouldPutAndGetSameData(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldPutAndGetSameData"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    Assert.assertTrue(c.containsKey(key));
	    Assert.assertEquals(value, (String)c.get(key));

	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

@Test
    public void shouldPutAndGetAndDeleteData(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldPutAndGetAndDeleteData"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.delete(key);
	    Assert.assertFalse(c.containsKey(key));
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }


@Test
    public void shouldPutAndNotGetAfterCloseAndReopenOverwrite(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldPutAndGetAfterCloseAndReopenOverwrite"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.close();

	    c = (BDBCache)TCacheManager.instance(p);
	    Assert.assertFalse(c.containsKey(key));
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

@Test
    public void shouldPutAndGetAfterCloseAndReopen(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldPutAndGetAfterCloseAndReopenOverwrite"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "false");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.close();

	    c = (BDBCache)TCacheManager.instance(p);
	    Assert.assertTrue(c.containsKey(key));
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

@Test
    public void shouldPutAndOverwriteKey(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("goodDirPutGetOverwrite"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    Assert.assertEquals(value, (String)c.get(key));
	    c.put(key, value2);
	    Assert.assertEquals(value2, (String)c.get(key));

	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

@Test
public void shouldZeroTimeToLiveShouldEvict(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldZeroTimeToLiveShouldEvict"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "0");

	    c = (BDBCache)TCacheManager.instance(p);
	    String ev = "_evict";
	    c.put(key+ev, value+ev);
	    Assert.assertFalse(c.containsKey(key+ev));
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }


@Test
public void shouldCountZeroItemsOnStart(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldCountZeroItemsOnStart"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "0");

	    c = (BDBCache)TCacheManager.instance(p);
	    Assert.assertEquals(0, c.size());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

@Test
public void shouldCountTwoItemsAfterAddingTwoItems(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldCountTwoItemsAfterAddingTwoItems"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "0");

	    c = (BDBCache)TCacheManager.instance(p);
	    c.put(key,value);
	    c.put(key+"z", value2);
	    Assert.assertEquals(2, c.size());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

    @Test
    public void shouldHaveZeroEntriesAfterPutAndClear(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldHaveZeroEntriesAfterPutAndClear"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "false");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.put(key2, value2);
	    c.clear();
	    Assert.assertEquals(0, c.size());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

    @Test
    public void shouldHaveOneEntryAfterPutAndClearAndPut(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldHaveTwoEntriesAfterPutAndClearAndPut"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.put(key2, value2);
	    c.clear();
	    c.put(key2, value2);
	    Assert.assertEquals(1, c.size());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

    @Test
    public void shouldHaveHitTrueAfterPutGet(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldHaveHitTrueAfterPutGet"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.get(key);
	    Assert.assertEquals(true, c.lastGetAHit());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }

    @Test
    public void shouldHaveHitFalseAfterPut(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldHaveHitTrueAfterPutGet"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    Assert.assertEquals(false, c.lastGetAHit());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }


    @Test
    public void shouldHaveHitFalseAfterGetWithUnknownkey(){
	BDBCache c = null;
	try{
	    Properties p = new Properties();
	    p.setProperty(BDBCache.DB_DIR_KEY, tmpDir("shouldHaveHitTrueAfterPutGet"));
	    p.setProperty(BDBCache.OVERWRITE_KEY, "true");
	    p.setProperty(BDBCache.TTL_MINUTES_KEY, "99999");

	    c = (BDBCache)TCacheManager.instance(p);

	    c.put(key, value);
	    c.get(key2);
	    Assert.assertEquals(false, c.lastGetAHit());
	    c.close();
	}
	catch(Throwable t){
	    t.printStackTrace();
	    Assert.fail("This should not happen");
	}
	finally{
	    try{
		if(c != null){
		    c.close();
		}
	    }
	    catch(Throwable t){

	    }
	}
    }


}
