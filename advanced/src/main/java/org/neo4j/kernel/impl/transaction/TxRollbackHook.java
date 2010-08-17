package org.neo4j.kernel.impl.transaction;

public interface TxRollbackHook
{
    void rollbackTransaction( int eventIdentifier );
    
    void doneCommitting( int eventIdentifier );
}
