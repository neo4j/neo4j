package org.neo4j.kernel.impl.transaction;

import javax.transaction.Transaction;

public interface TxFinishHook
{
    boolean hasAnyLocks( Transaction tx );
    
    void finishTransaction( int eventIdentifier );
}
