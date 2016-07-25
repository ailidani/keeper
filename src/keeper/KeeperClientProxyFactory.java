package keeper;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;

public class KeeperClientProxyFactory implements ClientProxyFactory {

    @Override
    public ClientProxy create(String id) {
        return new KeeperClientProxy(KVService.NAME);
    }
}
