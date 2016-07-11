package keeper;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupOperation;

import java.io.IOException;

public class RemoveBackupOperation extends AbstractOperation implements BackupOperation {

    private Data key;
    private byte[] time;

    public RemoveBackupOperation() {}

    public RemoveBackupOperation(Data key) {
        this.key = key;
        this.time = Clock.now();
    }

    @Override
    public void run() throws Exception {
        Clock.update(time);
        KVService service = getService();
        Container container = service.containers[getPartitionId()];
        Data oldValue = container.remove(key);
        service.log().append(new LogEntry(time, key, oldValue, null));
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(key);
        out.write(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readData();
        time = in.readByteArray();
    }

}
