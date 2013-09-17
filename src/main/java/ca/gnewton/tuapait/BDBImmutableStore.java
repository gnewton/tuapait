package ca.gnewton.tuapait;

import java.io.Serializable;
import java.util.Properties;

public class BDBImmutableStore extends BDBStore{
    @Override
    public void init(Properties p) {
	p.setProperty(OVERWRITE_KEY, "false");
	p.setProperty(READ_ONLY_KEY, "true");
	super.init(p);
    }

    @Override
    public boolean put(String key, Serializable rec){
	return false;
    }

    @Override
    public boolean delete(String key){
	return false;
    }

    @Override
    public boolean clear(){
	return false;
    }
}
