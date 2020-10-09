package org.xaplus;

import javax.transaction.xa.XAResource;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlusTransaction {

    private final XAPlusXid xid;
    private final String serverId;
    private final String superiorServerId;
    private final long expireTimeInMillis;
    private final Map<XAPlusXid, XAResource> xaResources;
    private final Map<XAPlusXid, XAPlusResource> xaPlusResources;
    private final Map<XAPlusXid, String> uniqueNames;
    private final XAPlusFuture future;

    XAPlusTransaction(XAPlusXid xid, int timeoutInSeconds, String serverId) {
        this.xid = xid;
        this.serverId = serverId;
        superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
        expireTimeInMillis = System.currentTimeMillis() + timeoutInSeconds * 1000;
        xaResources = new ConcurrentHashMap<>();
        xaPlusResources = new ConcurrentHashMap<>();
        uniqueNames = new ConcurrentHashMap<>();
        future = new XAPlusFuture();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
                + "=(superiorServerId=" + superiorServerId
                + ", isSuperior=" + isSuperior()
                + ", isSubordinate=" + isSubordinate()
                + ", " + (expireTimeInMillis - System.currentTimeMillis()) + " ms to expire"
                + ", enlisted " + xaResources.size() + " XA and " + xaPlusResources.size() + " XA+ resources"
                + ", xid=" + xid;
    }

    XAPlusXid getXid() {
        return xid;
    }

    long getExpireTimeInMillis() {
        return expireTimeInMillis;
    }

    Map<XAPlusXid, XAResource> getXaResources() {
        return Collections.unmodifiableMap(xaResources);
    }

    Map<XAPlusXid, XAPlusResource> getXaPlusResources() {
        return Collections.unmodifiableMap(xaPlusResources);
    }

    Map<XAPlusXid, String> getUniqueNames() {
        return Collections.unmodifiableMap(uniqueNames);
    }

    XAPlusFuture getFuture() {
        return future;
    }

    void enlist(XAPlusXid branchXid, String uniqueName, XAResource resource) {
        uniqueNames.put(branchXid, uniqueName);
        xaResources.put(branchXid, resource);
    }

    void enlist(XAPlusXid branchXid, String serverId, XAPlusResource resource) {
        uniqueNames.put(branchXid, serverId);
        xaPlusResources.put(branchXid, resource);
    }

    boolean isSubordinate() {
        return !superiorServerId.equals(serverId);
    }

    boolean isSuperior() {
        return superiorServerId.equals(serverId);
    }
}
