package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;
import keeper.Clock;
import keeper.Container;
import keeper.KVService;
import keeper.LogEntry;

import java.io.IOException;

public class PutBackupOperation extends AbstractOperation implements BackupOperation {

    private Data key, value;
    private byte[] time;

    public PutBackupOperation() {}

    public PutBackupOperation(Data key, Data value) {
        this.key = key;
        this.value = value;
        this.time = Clock.now();
    }

    @Override
    public void run() throws Exception {
        Clock.update(time);
        KVService service = getService();
        Container container = service.containers[getPartitionId()];
        Data oldValue = container.put(key, value);
        service.log().append(new LogEntry(time, key, oldValue, value));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
        out.writeData(value);
        out.writeByteArray(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
        value = in.readData();
        time = in.readByteArray();
    }

}
