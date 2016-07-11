package keeper;

import com.hazelcast.nio.serialization.Data;

public class LogEntry {

    private byte[] time;
    private Data fromV;
    private Data toV;
    private Data key;

    private LogEntry next;
    private LogEntry prev;

    public LogEntry(byte[] time, Data key, Data fromV, Data toV) {
        this.time = time;
        this.key = key;
        this.fromV = fromV;
        this.toV = toV;
    }

    public byte[] getTime() {
        return time;
    }

    public Data getFromV() {
        return fromV;
    }

    public Data getToV() {
        return toV;
    }

    public Data getKey() {
        return key;
    }

    public LogEntry getNext() {
        return next;
    }

    public LogEntry setNext(LogEntry next) {
        this.next = next;
        return this;
    }

    public LogEntry getPrev() {
        return prev;
    }

    public LogEntry setPrev(LogEntry prev) {
        this.prev = prev;
        return this;
    }

    public boolean isHead() {
        return prev == null;
    }

    public boolean isTail() {
        return next == null;
    }

}
