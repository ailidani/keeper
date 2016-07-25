package keeper;

import com.hazelcast.client.spi.ClientProxy;

public class KeeperClientProxy extends ClientProxy implements KV {

    public KeeperClientProxy(String service) {
        super(service, "kvs");
    }

    @Override
    public Object get(Object key) {
        return null;
    }

    @Override
    public Object put(Object key, Object value) {
        return null;
    }

    @Override
    public Object remove(Object key) {
        return null;
    }

    @Override
    public boolean snapshot() {
        return false;
    }
}
