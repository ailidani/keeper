package keeper;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

public class KVProxy<K, V> extends AbstractDistributedObject<KVService> implements KV<K, V> {

    public KVProxy(NodeEngine nodeEngine, KVService kvService) {
        super(nodeEngine, kvService);
    }

    @Override
    public String getServiceName() {
        return KVService.NAME;
    }

    @Override
    public V get(K key) {
        Data k = toData(key);
        NodeEngine nodeEngine = getNodeEngine();
        GetOperation operation = new GetOperation(k);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(k);
        InvocationBuilder builder = nodeEngine.getOperationService().createInvocationBuilder(KVService.NAME, operation, partitionId);
        try {
            final Future<V> future = builder.invoke();
            return future.get();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        Data k = toData(key);
        Data v = toData(value);
        NodeEngine nodeEngine = getNodeEngine();
        PutOperation operation = new PutOperation(k, v);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(k);
        InvocationBuilder builder = nodeEngine.getOperationService().createInvocationBuilder(KVService.NAME, operation, partitionId);
        try {
            final Future<V> future = builder.invoke();
            return future.get();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return null;
    }

    @Override
    public V remove(K key) {
        Data k = toData(key);
        NodeEngine nodeEngine = getNodeEngine();
        RemoveOperation operation = new RemoveOperation(k);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(k);
        InvocationBuilder builder = nodeEngine.getOperationService().createInvocationBuilder(KVService.NAME, operation, partitionId);
        try {
            final Future<V> future = builder.invoke();
            return future.get();
        } catch (Exception e) {
            ExceptionUtil.rethrow(e);
        }
        return null;
    }

    @Override
    public String getName() {
        return "kvs";
    }

}
