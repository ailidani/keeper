package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import keeper.Clock;

import java.io.IOException;

// Based class for all operations??
public abstract class KVOperation extends AbstractOperation {

    private byte[] time = Clock.now();

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.write(time);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        time = in.readByteArray();
    }

}
