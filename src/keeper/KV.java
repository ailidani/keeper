package keeper;

import com.hazelcast.core.DistributedObject;

public interface KV<K, V> extends DistributedObject {

    V get(K key);

    V put(K key, V value);

    V remove(K key);

}
