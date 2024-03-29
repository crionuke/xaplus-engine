package org.xaplus.engine;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

/**
 * Based on code by Ludovic Orban
 * https://github.com/bitronix/btm/blob/master/btm/src/main/java/bitronix/tm/utils/Decoder.java
 *
 * @since 1.0.0
 */
class XAPlusConstantsDecoder {

    static String decodeXAExceptionErrorCode(XAException ex) {
        switch (ex.errorCode) {
            // rollback errors
            case XAException.XA_RBROLLBACK:
                return "XA_RBROLLBACK";
            case XAException.XA_RBCOMMFAIL:
                return "XA_RBCOMMFAIL";
            case XAException.XA_RBDEADLOCK:
                return "XA_RBDEADLOCK";
            case XAException.XA_RBTRANSIENT:
                return "XA_RBTRANSIENT";
            case XAException.XA_RBINTEGRITY:
                return "XA_RBINTEGRITY";
            case XAException.XA_RBOTHER:
                return "XA_RBOTHER";
            case XAException.XA_RBPROTO:
                return "XA_RBPROTO";
            case XAException.XA_RBTIMEOUT:
                return "XA_RBTIMEOUT";

            // heuristic errors
            case XAException.XA_HEURCOM:
                return "XA_HEURCOM";
            case XAException.XA_HEURHAZ:
                return "XA_HEURHAZ";
            case XAException.XA_HEURMIX:
                return "XA_HEURMIX";
            case XAException.XA_HEURRB:
                return "XA_HEURRB";

            // misc failures errors
            case XAException.XAER_RMERR:
                return "XAER_RMERR";
            case XAException.XAER_RMFAIL:
                return "XAER_RMFAIL";
            case XAException.XAER_NOTA:
                return "XAER_NOTA";
            case XAException.XAER_INVAL:
                return "XAER_INVAL";
            case XAException.XAER_PROTO:
                return "XAER_PROTO";
            case XAException.XAER_ASYNC:
                return "XAER_ASYNC";
            case XAException.XAER_DUPID:
                return "XAER_DUPID";
            case XAException.XAER_OUTSIDE:
                return "XAER_OUTSIDE";

            default:
                return "!invalid error code (" + ex.errorCode + ")!";
        }
    }

    static String decodePrepareVote(int vote) {
        switch (vote) {
            case XAResource.XA_OK:
                return "XA_OK";
            case XAResource.XA_RDONLY:
                return "XA_RDONLY";
            default:
                return "!invalid return code (" + vote + ")!";
        }
    }
}
