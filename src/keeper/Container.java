package keeper;

import com.hazelcast.nio.serialization.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Container {

    private final ConcurrentMap<Data, Data> kvs = new ConcurrentHashMap<>(1000);

    Data get(Data key) {
        return kvs.get(key);
    }

    // TODO write to a local log
    Data put(Data key, Data value) {
        return kvs.put(key, value);
    }

    Data remove(Data key) {
        return kvs.remove(key);
    }

    void clear() {
        kvs.clear();
    }

    void applyMigrationData(Map<Data, Data> migrationData) {
        kvs.putAll(migrationData);
    }

    Map<Data, Data> toMigrationData() {
        return new HashMap<>(kvs);
    }

}
