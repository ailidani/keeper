package keeper;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public final class Clock {

    public static final String CLOCK_EPS = "clock.eps";
    public static final String CLOCK_TYPE = "clock.type";
    public static final String CLOCK_SELF = "clock.self";

    private static final HClock CLOCK;

    public enum TYPE {HLC, HVC}
    private static TYPE type;
    private static long pt = 0;

    private Clock() {}

    public abstract static class HClock {
        protected abstract byte[] now();
        protected abstract void update(byte[] that);
    }

    static {
        CLOCK = initClock();
    }

    private static HClock initClock() {
        type = TYPE.valueOf(System.getProperty(CLOCK_TYPE, "HVC"));
        long eps;
        String eps_str = System.getProperty(CLOCK_EPS);
        if (eps_str != null) {
            eps = Long.parseLong(eps_str);
        } else {
            eps = Long.MAX_VALUE;
        }
        String self = System.getProperty(CLOCK_SELF);
        switch (type) {
            case HVC:
                return new HVC(self, eps);
            case HLC:
            default:
                // unknown type
                return null;
        }
    }


    public static synchronized long PT() {
        pt = System.currentTimeMillis();
        return pt;
    }

    public static synchronized byte[] now() {
        return CLOCK.now();
    }

    public static synchronized void update(byte[] that) {
        CLOCK.update(that);
    }

    public static TYPE getType() {
        return type;
    }

    private static final class HLC extends HClock {
        private long walltime;
        private long logical;

        protected byte[] now() {
            PT();
            if (walltime > pt) {
                logical++;
            } else {
                walltime = pt;
                logical = 0;
            }
            ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
            buffer.putLong(walltime);
            buffer.putLong(logical);
            return buffer.array();
        }

        protected void update(byte[] that) {
            PT();
            ByteBuffer buffer = ByteBuffer.allocate(2 * Long.BYTES);
            buffer.put(that);
            buffer.flip();
            long thatW = buffer.getLong();
            long thatL = buffer.getLong();
            if (pt > walltime && pt > thatW) {
                walltime = pt;
                logical = 0;
                return;
            }
            if (thatW > walltime) {
                walltime = thatW;
                logical = thatL + 1;
            } else if (thatW < walltime) {
                logical++;
            } else {
                if (thatL > logical)
                    logical = thatL;
                logical++;
            }
        }
    }

    private static final class HVC extends HClock {
        private static Map<String, Long> hvc = new HashMap<>();
        private static String self;
        private static long eps = Long.MAX_VALUE;

        private HVC(String s, long e) {
            self = s;
            eps = e;
        }

        protected byte[] now() {
            hvc.put(self, PT());
            clean();
            System.out.println("now() " + hvc);
            try {
                // Convert Map to byte array
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(byteOut);
                out.writeObject(hvc);
                return byteOut.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void update(byte[] that) {
            try {
                // Parse byte array to Map
                ByteArrayInputStream byteIn = new ByteArrayInputStream(that);
                ObjectInputStream in = new ObjectInputStream(byteIn);
                Map<String, Long> vc = (Map<String, Long>) in.readObject();
                System.out.println("update() with " + vc);
                for (Map.Entry<String, Long> e : vc.entrySet()) {
                    if (!hvc.containsKey(e.getKey()) || hvc.get(e.getKey()) < e.getValue()) {
                        hvc.put(e.getKey(), e.getValue());
                    }
                }
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            clean();
        }

        private static void clean() {
            for (String uuid : hvc.keySet()) {
                if (hvc.get(uuid) <= pt - eps) {
                    hvc.remove(uuid);
                }
            }
        }

    }

}
