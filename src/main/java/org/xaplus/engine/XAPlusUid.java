package org.xaplus.engine;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Ludovic Orban
 * @author Byvshev Kirill
 * @since 1.0.0
 */
public final class XAPlusUid {

    static private final AtomicInteger sequenceGenerator = new AtomicInteger();
    static private final AtomicLong lastTime = new AtomicLong(Long.MIN_VALUE);
    static private final char[] HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private final String serverId;
    private final long timestamp;
    private final int sequence;

    private final byte[] array;
    private final int hashCodeValue;
    private final String toStringValue;

    XAPlusUid(byte[] bytes) {
        // TODO: validate bytes length

        byte[] copy = new byte[bytes.length];
        System.arraycopy(bytes, 0, copy, 0, bytes.length);
        this.array = copy;

        this.serverId = extractServerId(bytes);
        this.timestamp = extractTimestamp(bytes);
        this.sequence = extractSequence(bytes);

        this.hashCodeValue = arrayHashCode(bytes);
        this.toStringValue = arrayToHex(bytes);
    }

    XAPlusUid(String serverId) {
        // TODO: validate serverId length

        this.serverId = serverId;
        this.timestamp = getCurrentMonotonicTimeMillis();
        this.sequence = sequenceGenerator.incrementAndGet();

        byte[] serverIdBytes = serverId.getBytes();
        byte[] timestampBytes = longToBytes(timestamp);
        byte[] sequenceBytes = intToBytes(sequence);

        int length = serverIdBytes.length + timestampBytes.length + sequenceBytes.length;
        byte[] bytes = new byte[length];

        System.arraycopy(serverIdBytes, 0, bytes, 0, serverIdBytes.length);
        System.arraycopy(timestampBytes, 0, bytes, serverIdBytes.length, timestampBytes.length);
        System.arraycopy(sequenceBytes, 0, bytes, serverIdBytes.length + timestampBytes.length, sequenceBytes.length);

        this.array = bytes;
        this.hashCodeValue = arrayHashCode(bytes);
        this.toStringValue = arrayToHex(bytes);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof XAPlusUid) {
            XAPlusUid otherXAPlusUid = (XAPlusUid) obj;

            // Optimizes performance a bit
            if (hashCodeValue != otherXAPlusUid.hashCodeValue) {
                return false;
            }

            return Arrays.equals(array, otherXAPlusUid.array);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return hashCodeValue;
    }

    @Override
    public String toString() {
        return toStringValue;
    }

    byte[] getArray() {
        return array;
    }

    String getServerId() {
        return serverId;
    }

    long getTimestamp() {
        return timestamp;
    }

    int getSequence() {
        return sequence;
    }

    private String extractServerId(byte[] bytes) {
        int serverIdLength = bytes.length - Integer.BYTES - Long.BYTES;
        if (serverIdLength < 1)
            return null;

        byte[] result = new byte[serverIdLength];
        System.arraycopy(bytes, 0, result, 0, serverIdLength);
        return new String(result);
    }

    private long extractTimestamp(byte[] bytes) {
        return bytesToLong(bytes, bytes.length - Integer.BYTES - Long.BYTES);
    }

    private int extractSequence(byte[] bytes) {
        return bytesToInt(bytes, bytes.length - Integer.BYTES);
    }

    private long bytesToLong(byte[] bytes, int pos) {
        long result = 0;

        for (int i = 0; i < 8; i++) {
            result <<= 8;
            result ^= (long) bytes[i + pos] & 0xFF;
        }

        return result;
    }

    private int bytesToInt(byte[] bytes, int pos) {
        int result = 0;

        for (int i = 0; i < 4; i++) {
            result <<= 8;
            result ^= (int) bytes[i + pos] & 0xFF;
        }

        return result;
    }

    byte[] longToBytes(long aLong) {
        byte[] array = new byte[8];

        array[7] = (byte) (aLong & 0xff);
        array[6] = (byte) ((aLong >> 8) & 0xff);
        array[5] = (byte) ((aLong >> 16) & 0xff);
        array[4] = (byte) ((aLong >> 24) & 0xff);
        array[3] = (byte) ((aLong >> 32) & 0xff);
        array[2] = (byte) ((aLong >> 40) & 0xff);
        array[1] = (byte) ((aLong >> 48) & 0xff);
        array[0] = (byte) ((aLong >> 56) & 0xff);

        return array;
    }

    byte[] intToBytes(int anInt) {
        byte[] array = new byte[4];

        array[3] = (byte) (anInt & 0xff);
        array[2] = (byte) ((anInt >> 8) & 0xff);
        array[1] = (byte) ((anInt >> 16) & 0xff);
        array[0] = (byte) ((anInt >> 24) & 0xff);

        return array;
    }

    private int arrayHashCode(byte[] bytes) {
        int hash = 0;
        // Common fast but good hash with wide dispersion
        for (int i = bytes.length - 1; i > 0; i--) {
            // rotate left and xor
            // (very fast in assembler, a bit clumsy in Java)
            hash <<= 1;

            if (hash < 0) {
                hash |= 1;
            }

            hash ^= bytes[i];
        }
        return hash;
    }

    private String arrayToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        int c = 0;
        int v;
        for (int i = 0; i < bytes.length; i++) {
            v = bytes[i] & 0xFF;
            hexChars[c++] = HEX[v >> 4];
            hexChars[c++] = HEX[v & 0xF];
        }
        return new String(hexChars);
    }

    private long getCurrentMonotonicTimeMillis() {
        long now = System.currentTimeMillis();
        long time = lastTime.get();
        if (now > time) {
            lastTime.compareAndSet(time, now);
            return lastTime.get();
        }
        // Return the current time in milliseconds, guaranteeing monotonic time increment.
        return time;
    }
}

