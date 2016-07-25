package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.serialization.SerializationService;
import keeper.Clock;
import keeper.KVService;
import keeper.LogEntry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SnapshotOperation extends AbstractOperation {

    private byte[] time;
    private boolean successful;

    public SnapshotOperation() {}

    public SnapshotOperation(byte[] time) {
        this.time = time;
    }

    @Override
    public void run() throws Exception {
        KVService service = getService();
        int partitionId = getPartitionId();
        Map<Data, Data> kvs = service.containers[partitionId].snapshot();
        LogEntry entry = service.log().tail();
        while (entry != null && Clock.compare(entry.getTime(), time) >= 0) {
            Data key = entry.getKey();
            Data value = entry.getFromV();
            kvs.put(key, value);
            entry = entry.getPrev();
        }
        SerializationService ss = getNodeEngine().getSerializationService();
        Map<Integer, Integer> map = new HashMap<>();
        for (Map.Entry e : kvs.entrySet()) {
            map.put(ss.toObject(e.getKey()), ss.toObject(e.getValue()));
        }
        System.out.println(map);
        successful = true;
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return successful;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByteArray(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        time = in.readByteArray();
    }
}
