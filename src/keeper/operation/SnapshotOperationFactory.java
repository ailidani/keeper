package keeper.operation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;

import java.io.IOException;

public class SnapshotOperationFactory implements OperationFactory {

    private byte[] time;

    public SnapshotOperationFactory() { }

    public SnapshotOperationFactory(byte[] time) {
        this.time = time;
    }

    @Override
    public Operation createOperation() {
        return new SnapshotOperation(time);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeByteArray(time);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        time = in.readByteArray();
    }
}
