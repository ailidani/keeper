package keeper;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.client.impl.protocol.task.map.AbstractMapPutMessageTask;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.Operation;
import keeper.operation.PutOperation;

public class PutMessageTask extends AbstractMapPutMessageTask<MapPutCodec.RequestParameters> {

    public PutMessageTask(ClientMessage clientMessage, com.hazelcast.instance.Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PutOperation(parameters.key, parameters.value);
    }

    @Override
    protected MapPutCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapPutCodec.encodeResponse(serializationService.toData(response));
    }

    @Override
    public String getDistributedObjectName() {
        return parameters.name;
    }

    @Override
    public String getMethodName() {
        return "put";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{parameters.key, parameters.value};
    }
}
