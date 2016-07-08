package keeper;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.*;
import com.hazelcast.spi.partition.MigrationEndpoint;

import java.util.Map;
import java.util.Properties;

public class KVService implements ManagedService, RemoteService, MigrationAwareService {

    public static final String NAME = "KVService";
    Container[] containers;
    private NodeEngine nodeEngine;

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        this.nodeEngine = nodeEngine;
        containers = new Container[nodeEngine.getPartitionService().getPartitionCount()];
        for (int i=0; i<containers.length; i++) {
            containers[i] = new Container();
        }
    }

    @Override
    public void reset() {}

    @Override
    public void shutdown(boolean terminate) {}

    @Override
    public DistributedObject createDistributedObject(String name) {
        // return nodeEngine.getHazelcastInstance().getMap(name);
        return new KVProxy(nodeEngine, this);
    }

    @Override
    public void destroyDistributedObject(String name) {}

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent e) {
        if (e.getReplicaIndex() > 1) {
            return null;
        }
        Container container = containers[e.getPartitionId()];
        Map<Data, Data> data = container.toMigrationData();
        return data.isEmpty() ? null : new MigrationOperation(data);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent e) {}

    @Override
    public void commitMigration(PartitionMigrationEvent e) {
        if (e.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            Container container = containers[e.getPartitionId()];
            container.clear();
        }
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent e) {
        if (e.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            Container container = containers[e.getPartitionId()];
            container.clear();
        }
    }

}
