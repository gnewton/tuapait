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

    public static final String DB_DIR_KEY= "dbdir.TCache";
    public static final String OVERWRITE_KEY = "overwrite.TCache";
    public static final String READ_ONLY_KEY= "readOnly.TCache";
    public static final String TTL_MINUTES_KEY= "timeToLiveMinutes.TCache";
    public static final String KEY_ENCODING_KEY = "keyEncoding.TCache";

    public static final String DEFAULT_CACHE_NAME = "bdbcache";

    public static final String BDB_DB_NAME = "ca.gnewton.tuapait.TCache";
    public static final String BDB_LOG_FILE_SIZE_MB = "logSize.ca.gnewton.tuapait.TCache";

}
