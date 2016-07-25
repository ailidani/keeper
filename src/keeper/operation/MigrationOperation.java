package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import keeper.Clock;
import keeper.Container;
import keeper.KVService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MigrationOperation extends AbstractOperation {

    Map<Data, Data> migrationData;
    private byte[] time;

    public MigrationOperation() {}

    public MigrationOperation(Map<Data, Data> migrationData) {
        this.migrationData = migrationData;
        this.time = Clock.now();
    }

    @Override
    public void run() throws Exception {
        Clock.update(time);
        KVService service = getService();
        Container container = service.containers[getPartitionId()];
        container.applyMigrationData(migrationData);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(migrationData.size());
        for (Map.Entry<Data, Data> entry : migrationData.entrySet()) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
        out.writeByteArray(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int size = in.readInt();
        migrationData = new HashMap<>();
        for (int i=0; i<size; i++) {
            migrationData.put(in.readData(), in.readData());
        }
        time = in.readByteArray();
    }
}
