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

/**
 * @author Ludovic Orban
 * @since 1.0.0
 */
public class XAPlusXid implements javax.transaction.xa.Xid {

    /**
     * int-encoded "Btnx" string. This is used as the globally unique ID to discriminate BTM XIDs.
     */
    static final int FORMAT_ID = 0x42746e78;

    /**
     * Decode XID from string representation
     *
     * @param xidString string to decode
     * @return {@link XAPlusXid}
     */
    static public XAPlusXid fromString(String xidString) {
        if (xidString == null) {
            throw new NullPointerException("xid is null");
        }
        String[] gtridBqual = xidString.split(":");
        if (gtridBqual.length == 2) {
            XAPlusUid globalTransactionId = new XAPlusUid(XAPlusArraysEncoderDecoder.hexToArray(gtridBqual[0]));
            XAPlusUid branchQualifier = new XAPlusUid(XAPlusArraysEncoderDecoder.hexToArray(gtridBqual[1]));
            return new XAPlusXid(globalTransactionId, branchQualifier);
        } else {
            throw new IllegalArgumentException("Wrong xid=" + xidString + " to decoder");
        }
    }

    /**
     * Generate a new XID based on gtrid and new bqual for serverId.
     *
     * @param gtrid    the GTRID to use to generate the xid
     * @param serverId who will execute branch, used for bqual generation
     * @return the generated xid.
     */
    static XAPlusXid generate(XAPlusUid gtrid, String serverId) {
        return new XAPlusXid(gtrid, XAPlusUid.generate(serverId));
    }

    private final XAPlusUid globalTransactionId;
    private final XAPlusUid branchQualifier;
    private final int hashCodeValue;
    private final String toStringValue;

    /**
     * Create a new XID using the specified GTRID and BQUAL.
     *
     * @param globalTransactionId the GTRID.
     * @param branchQualifier     the BQUAL.
     */
    XAPlusXid(XAPlusUid globalTransactionId, XAPlusUid branchQualifier) {
        if (globalTransactionId == null) {
            throw new NullPointerException("Global transaction id is null");
        }
        if (branchQualifier == null) {
            throw new NullPointerException("Branch qualifier is null");
        }
        this.globalTransactionId = globalTransactionId;
        this.branchQualifier = branchQualifier;
        this.toStringValue = precalculateToString();
        this.hashCodeValue = precalculateHashCode();
    }

    XAPlusXid(javax.transaction.xa.Xid xid) {
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.globalTransactionId = new XAPlusUid(xid.getGlobalTransactionId());
        this.branchQualifier = new XAPlusUid(xid.getBranchQualifier());
        this.toStringValue = precalculateToString();
        this.hashCodeValue = precalculateHashCode();
    }

    /**
     * Get Bitronix XID format ID. Defined by {@link XAPlusXid#FORMAT_ID}.
     *
     * @return the Bitronix XID format ID.
     */
    @Override
    public int getFormatId() {
        return FORMAT_ID;
    }

    /**
     * Get the BQUAL of the XID.
     *
     * @return the XID branch qualifier.
     */
    @Override
    public byte[] getBranchQualifier() {
        return branchQualifier.getArray();
    }

    XAPlusUid getBranchQualifierUid() {
        return branchQualifier;
    }

    /**
     * Get the GTRID of the XID.
     *
     * @return the XID global transaction ID.
     */
    @Override
    public byte[] getGlobalTransactionId() {
        return globalTransactionId.getArray();
    }

    XAPlusUid getGlobalTransactionIdUid() {
        return globalTransactionId;
    }

    /**
     * Get a human-readable string representation of the XID.
     *
     * @return a human-readable string representation.
     */
    @Override
    public String toString() {
        return toStringValue;
    }

    private String precalculateToString() {
        return globalTransactionId.toString() + ":" + branchQualifier.toString();
    }

    /**
     * Compare two XIDs for equality.
     *
     * @param obj the XID to compare to.
     * @return true if both XIDs have the same format ID and contain exactly the same GTRID and BQUAL.
     */
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof XAPlusXid))
            return false;

        XAPlusXid otherXAPlusXid = (XAPlusXid) obj;
        return FORMAT_ID == otherXAPlusXid.getFormatId() &&
                globalTransactionId.equals(otherXAPlusXid.getGlobalTransactionIdUid()) &&
                branchQualifier.equals(otherXAPlusXid.getBranchQualifierUid());
    }

    /**
     * Get an integer hash for the XID.
     *
     * @return a constant hash value.
     */
    @Override
    public int hashCode() {
        return hashCodeValue;
    }

    private int precalculateHashCode() {
        int hashCode = FORMAT_ID;
        if (globalTransactionId != null)
            hashCode += globalTransactionId.hashCode();
        if (branchQualifier != null)
            hashCode += branchQualifier.hashCode();
        return hashCode;
    }
}
