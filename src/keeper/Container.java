package keeper;

import com.hazelcast.nio.serialization.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Container {

    private final ConcurrentMap<Data, Data> kvs = new ConcurrentHashMap<>(1000);

    public Data get(Data key) {
        return kvs.get(key);
    }

    // TODO write to a local log
    public Data put(Data key, Data value) {
        return kvs.put(key, value);
    }

    public Data remove(Data key) {
        return kvs.remove(key);
    }

    public void clear() {
        kvs.clear();
    }

    public void applyMigrationData(Map<Data, Data> migrationData) {
        kvs.putAll(migrationData);
    }

    public Map<Data, Data> toMigrationData() {
        return new HashMap<>(kvs);
    }

    public Map<Data, Data> snapshot() {
        return new HashMap<>(kvs);
    }

}
