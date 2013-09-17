package ca.gnewton.tuapait.example;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.Assert;
import java.util.List;
import java.util.ArrayList;



@RunWith(JUnit4.class)
public class SimpleTest{


    @Test
    public void shouldRunOK(){

	Simple simple = new Simple("k");
	simple.start();
	try{
	    simple.join();
	}catch(Throwable t){
	    t.printStackTrace();
	}

	Assert.assertTrue(true);
    }



    @Test
    public void shouldRunOKMultiThreaded(){
	List<Simple> simples = new ArrayList<Simple>();
	for(int i=0; i<5; i++){
	    Simple simple = new Simple(Integer.toString(i) + "_");
	    simple.start();
	    simples.add(simple);
	}

	for(Simple simple: simples){
	    try{
		simple.join();
	    }catch(Throwable t){
		t.printStackTrace();
	    }
	}
	simples = new ArrayList<Simple>();
	for(int i=0; i<5; i++){
	    Simple simple = new Simple(Integer.toString(i) + "_");
	    simple.start();
	    simples.add(simple);
	}

	for(Simple simple: simples){
	    try{
		simple.join();
	    }catch(Throwable t){
		t.printStackTrace();
	    }
	}
	Assert.assertTrue(true);
    }


}

