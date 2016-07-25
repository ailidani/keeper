package keeper;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ProxyFactoryConfig;
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
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config);

        System.setProperty("clock.self", instance1.getCluster().getLocalMember().getUuid());
        System.setProperty("clock.eps", "10000");

        KV<Integer, Integer> kv = instance1.getDistributedObject(KVService.NAME, "");
        KV<Integer, Integer> kv2 = instance2.getDistributedObject(KVService.NAME, "");

        for (int i=0; i<100; i++) {
            kv.put(i, i);
        }
        boolean successful = kv2.snapshot();
        assert successful;

        kv.put(1, 1);
        assert kv.get(1) == 1;
        assert kv2.put(1, 2) == 1;
        kv.remove(1);

        ClientConfig clientConfig = new ClientConfig();
        ProxyFactoryConfig proxyFactoryConfig = new ProxyFactoryConfig();
        proxyFactoryConfig.setService(KVService.NAME);
        proxyFactoryConfig.setClassName(KeeperClientProxyFactory.class.getName());
        clientConfig.addProxyFactoryConfig(proxyFactoryConfig);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        KV<Integer, Integer> kvc = client.getDistributedObject(KVService.NAME, "");
        kvc.put(2, 2);
        assert kvc.get(2) == 2;
        kvc.remove(2);

    }
}
