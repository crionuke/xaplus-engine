package org.xaplus.engine;

/**
 * @author Ludovic Orban
 * @author Byvshev Kirill
 * @since 1.0.0
 */
public final class XAPlusXid implements javax.transaction.xa.Xid {

    static final int FORMAT_ID = 0x42746e78;

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

    private final XAPlusUid gtrid;
    private final XAPlusUid bqual;

    private final int hashCodeValue;
    private final String toStringValue;

    XAPlusXid(javax.transaction.xa.Xid xid) {
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.gtrid = new XAPlusUid(xid.getGlobalTransactionId());
        this.bqual = new XAPlusUid(xid.getBranchQualifier());
        this.toStringValue = precalculateToString();
        this.hashCodeValue = precalculateHashCode();
    }

    XAPlusXid(XAPlusUid gtrid, XAPlusUid bqual) {
        if (gtrid == null) {
            throw new NullPointerException("gtrid is null");
        }
        if (bqual == null) {
            throw new NullPointerException("bqual is null");
        }
        this.gtrid = gtrid;
        this.bqual = bqual;
        this.toStringValue = precalculateToString();
        this.hashCodeValue = precalculateHashCode();
    }

    XAPlusXid(XAPlusUid gtrid, String serverId) {
        this(gtrid, new XAPlusUid(serverId));
    }

    @Override
    public String toString() {
        return toStringValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof XAPlusXid))
            return false;

        XAPlusXid otherXAPlusXid = (XAPlusXid) obj;
        return FORMAT_ID == otherXAPlusXid.getFormatId() &&
                gtrid.equals(otherXAPlusXid.getGtrid()) &&
                bqual.equals(otherXAPlusXid.getBqual());
    }

    @Override
    public int hashCode() {
        return hashCodeValue;
    }


    @Override
    public int getFormatId() {
        return FORMAT_ID;
    }

    @Override
    public byte[] getBranchQualifier() {
        return bqual.getArray();
    }

    @Override
    public byte[] getGlobalTransactionId() {
        return gtrid.getArray();
    }

    XAPlusUid getBqual() {
        return bqual;
    }

    XAPlusUid getGtrid() {
        return gtrid;
    }

    private String precalculateToString() {
        return gtrid.toString() + ":" + bqual.toString();
    }

    private int precalculateHashCode() {
        return FORMAT_ID + gtrid.hashCode() + bqual.hashCode();
    }
}
