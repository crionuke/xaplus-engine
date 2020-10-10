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

/**
 * @author Ludovic Orban
 * @since 1.0.0
 */
final class XAPlusUid {

    private static final char[] HEX = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    private final byte[] array;
    private final int hashCodeValue;
    private final String toStringValue;

    XAPlusUid(String hex) {
        this(hexToArray(hex));
    }

    XAPlusUid(byte[] array) {
        this.array = new byte[array.length];
        System.arraycopy(array, 0, this.array, 0, array.length);
        this.hashCodeValue = arrayHashCode(array);
        this.toStringValue = arrayToHex(array);
    }

    /**
     * Compute a UID byte array hashcode value.
     *
     * @param uid the byte array used for hashcode computation.
     * @return a constant hash value for the specified uid.
     */
    private static int arrayHashCode(byte[] uid) {
        int hash = 0;
        // Common fast but good hash with wide dispersion
        for (int i = uid.length - 1; i > 0; i--) {
            // rotate left and xor
            // (very fast in assembler, a bit clumsy in Java)
            hash <<= 1;

            if (hash < 0) {
                hash |= 1;
            }

            hash ^= uid[i];
        }
        return hash;
    }

    /**
     * Decode a UID byte array into a (somewhat) human-readable hex string.
     *
     * @param uid the uid to decode.
     * @return the resulting printable string.
     */
    private static String arrayToHex(byte[] uid) {
        char[] hexChars = new char[uid.length * 2];
        int c = 0;
        int v;
        for (int i = 0; i < uid.length; i++) {
            v = uid[i] & 0xFF;
            hexChars[c++] = HEX[v >> 4];
            hexChars[c++] = HEX[v & 0xF];
        }
        return new String(hexChars);
    }

    private static byte[] hexToArray(String uid) {
        int len = uid.length();
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bytes[i / 2] = (byte) ((Character.digit(uid.charAt(i), 16) << 4)
                    + Character.digit(uid.charAt(i + 1), 16));
        }
        return bytes;
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
}

