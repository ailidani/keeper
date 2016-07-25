package keeper;

import java.io.File;

public class Log {

    private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
    private File directory = new File(DEFAULT_DIRECTORY);

    private LogEntry head;
    private LogEntry tail;

    private long length = 0;

    public Log() {}

    public synchronized void append(LogEntry entry) {
        if (head == null) {
            head = tail = entry;
            return;
        }
        tail.setNext(entry);
        entry.setPrev(tail);
        tail = entry;
        length++;
    }

    public long length() {
        return length;
    }

    public LogEntry tail() {
        return tail;
    }
}
