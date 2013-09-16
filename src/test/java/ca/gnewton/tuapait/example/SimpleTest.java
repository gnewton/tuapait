package ca.gnewton.tuapait.example;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Assert;




@RunWith(JUnit4.class)
public class SimpleTest{


    @Test
    public void shouldRunOK(){

	Simple simple = new Simple("k");
	simple.start();
	Assert.assertTrue(true);
    }



    @Test
    public void shouldRunOKMultiThreaded(){

	Simple simple = new Simple("1");
	simple.start();
	Simple simple2 = new Simple("2");
	simple2.start();
	try{
	    simple.join();
	    simple2.join();
	}catch(Throwable t){
	    t.printStackTrace();
	}
	Assert.assertTrue(true);
    }


}

