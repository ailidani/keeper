package keeper;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Node {

    public static void main(String[] args) {
        ServiceConfig serviceConfig = new ServiceConfig();
        serviceConfig.setEnabled(true);
        serviceConfig.setName(KVService.NAME);
        serviceConfig.setClassName("keeper.KVService");
        Config config = new Config();
        config.getServicesConfig().addServiceConfig(serviceConfig);
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        System.setProperty("clock.self", instance.getCluster().getLocalMember().getUuid());
        System.setProperty("clock.eps", "10000");

        KV<Integer, Integer> kv = instance.getDistributedObject(KVService.NAME, "");

        kv.put(1, 1);
        int x = kv.get(1);
        assert x == 1;
        kv.remove(1);
    }
}
