package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import keeper.Clock;
import keeper.Container;
import keeper.KVService;
import keeper.LogEntry;

import java.io.IOException;

public class RemoveOperation extends AbstractOperation implements PartitionAwareOperation, BackupAwareOperation {

    private Data key, oldValue;
    private byte[] time;

    public RemoveOperation() {}

    public RemoveOperation(Data key) {
        this.key = key;
        this.time = Clock.now();
    }

    @Override
    public void run() throws Exception {
        Clock.update(time);
        KVService service = getService();
        Container container = service.containers[getPartitionId()];
        oldValue = container.remove(key);
        service.log().append(new LogEntry(time, key, oldValue, null));
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return oldValue;
    }

    @Override
    public boolean shouldBackup() {
        return true;
    }

    @Override
    public int getSyncBackupCount() {
        return 1;
    }

    @Override
    public int getAsyncBackupCount() {
        return 0;
    }

    @Override
    public Operation getBackupOperation() {
        return new RemoveBackupOperation(key);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
        out.writeByteArray(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
        time = in.readByteArray();
    }
}
