package keeper.client;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapGetCodec;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.nio.serialization.Data;
import keeper.KV;

public class ClientKVProxy<K, V> extends ClientProxy implements KV<K, V> {

    public ClientKVProxy(String serviceName) {
        super(serviceName, "kvs");
    }

    @Override
    public V get(K key) {
        if (key == null) {
            throw new NullPointerException("Null key is not allowed!");
        }
        Data keyData = toData(key);
        ClientMessage request = MapGetCodec.encodeRequest("kvs", )
        return null;
    }

    @Override
    public V put(K key, V value) {
        return null;
    }

    @Override
    public V remove(K key) {
        return null;
    }

    @Override
    public boolean snapshot() {
        return false;
    }
}
