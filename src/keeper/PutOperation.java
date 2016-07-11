package keeper;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

public class PutOperation extends AbstractOperation implements PartitionAwareOperation, BackupAwareOperation {

    private Data key, value, oldValue;
    private byte[] time;

    public PutOperation() {}

    public PutOperation(Data key, Data value) {
        this.key = key;
        this.value = value;
        this.time = Clock.now();
    }

    @Override
    public void run() throws Exception {
        Clock.update(time);
        KVService service = getService();
        Container container = service.containers[getPartitionId()];
        oldValue = container.put(key, value);
        service.log().append(new LogEntry(time, key, oldValue, value));
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
        return new PutBackupOperation(key, value);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
        out.writeData(value);
        out.write(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
        value = in.readData();
        time = in.readByteArray();
    }

}
