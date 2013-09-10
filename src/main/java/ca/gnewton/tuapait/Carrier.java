package ca.gnewton.tuapait;

import java.io.Serializable;

public class Carrier implements Serializable{
    public long timeStampMinutes;
    public Object object;

    protected Carrier(){
	timeStampMinutes = System.currentTimeMillis()/1000/60;
    }
}
