package ca.gnewton.tuapait;

import java.io.Serializable;
import java.util.Properties;

public interface TCache{
    public void init(Properties p);
    public boolean delete(String key);
    public boolean put(String key, Serializable rec);
    public boolean containsKey(String key);
    public Object get(String key);
    public boolean close();
    public long size();
    public boolean clear();
}
