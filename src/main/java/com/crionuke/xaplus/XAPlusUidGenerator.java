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
package com.crionuke.xaplus;

import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 * @since 1.0.0
 */
@Component
class XAPlusUidGenerator {

    static private final AtomicInteger sequenceGenerator = new AtomicInteger();

    /**
     * @return the generated UID based on {@code serverId}
     */
    XAPlusUid generateUid(String serverId) {
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

    /**
     * Generate a new XID.
     *
     * @param gtrid    the GTRID to use to generate the xid
     * @param serverId who will execute branch, used for bqual generation
     * @return the generated xid.
     */
    XAPlusXid generateXid(XAPlusUid gtrid, String serverId) {
        return new XAPlusXid(gtrid, generateUid(serverId));
    }
}
