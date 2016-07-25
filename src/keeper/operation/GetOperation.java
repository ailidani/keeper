package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.ReadonlyOperation;
import keeper.Clock;
import keeper.Container;
import keeper.KVService;

import java.io.IOException;

public class GetOperation extends AbstractOperation implements PartitionAwareOperation, ReadonlyOperation {

    private Data key, value;
    private byte[] time;

    public GetOperation() {}

    public GetOperation(Data key) {
        this.key = key;
        this.time = Clock.now();
    }

    @Override
    public void run() throws Exception {
        Clock.update(time);
        KVService service = getService();
        Container container = service.containers[getPartitionId()];
        value = container.get(key);
    }

    @Override
    public boolean returnsResponse() {
        return true;
    }

    @Override
    public Object getResponse() {
        return value;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(key);
        out.write(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        key = in.readObject();
        time = in.readByteArray();
    }

    @Override
    public String getServiceName() {
        return KVService.NAME;
    }
}
