/*
 * Copyright (C) 2006-2013 Bitronix Software (http://www.bitronix.be)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xaplus.engine;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 * @since 1.0.0
 */
public final class XAPlusUid {

    static private final AtomicInteger sequenceGenerator = new AtomicInteger();
    private final byte[] array;
    private final int hashCodeValue;
    private final String toStringValue;

    XAPlusUid(byte[] array) {
        this.array = new byte[array.length];
        System.arraycopy(array, 0, this.array, 0, array.length);
        this.hashCodeValue = XAPlusArraysEncoderDecoder.arrayHashCode(array);
        this.toStringValue = XAPlusArraysEncoderDecoder.arrayToHex(array);
    }

    /**
     * Generate UID based on serverId
     *
     * @param serverId serverId to generate UID
     * @return
     */
    static XAPlusUid generate(String serverId) {
        byte[] timestamp = XAPlusNumbersEncoderDecoder.longToBytes(XAPlusMonotonicClock.currentTimeMillis());
        byte[] sequence = XAPlusNumbersEncoderDecoder.intToBytes(sequenceGenerator.incrementAndGet());
        byte[] serverIdBytes = serverId.getBytes();

        int uidLength = serverIdBytes.length + timestamp.length + sequence.length;
        byte[] uidArray = new byte[uidLength];

        System.arraycopy(serverIdBytes, 0, uidArray, 0, serverIdBytes.length);
        System.arraycopy(timestamp, 0, uidArray, serverIdBytes.length, timestamp.length);
        System.arraycopy(sequence, 0, uidArray, serverIdBytes.length + timestamp.length, sequence.length);

        return new XAPlusUid(uidArray);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof XAPlusUid) {
            XAPlusUid otherXAPlusUid = (XAPlusUid) obj;

            // optimizes performance a bit
            if (hashCodeValue != otherXAPlusUid.hashCodeValue)
                return false;

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

    String extractServerId() {
        int serverIdLength = array.length - 4 - 8; // - sequence - timestamp
        if (serverIdLength < 1)
            return null;

        byte[] result = new byte[serverIdLength];
        System.arraycopy(array, 0, result, 0, serverIdLength);
        return new String(result);
    }

    long extractTimestamp() {
        return XAPlusNumbersEncoderDecoder.bytesToLong(array, array.length - 4 - 8); // - sequence - timestamp
    }

    int extractSequence() {
        return XAPlusNumbersEncoderDecoder.bytesToInt(array, array.length - 4); // - sequence
    }

    int length() {
        return array.length;
    }
}

