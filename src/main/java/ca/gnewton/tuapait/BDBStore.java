package ca.gnewton.tuapait;

import java.io.Serializable;

public class BDBStore extends BDBCache{
    
    @Override
    protected boolean timeToEvict(final String key, final long timeStampMinutes){
	return false;
    }

}
