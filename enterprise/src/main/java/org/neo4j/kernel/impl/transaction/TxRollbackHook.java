package org.neo4j.kernel.impl.transaction;

public interface TxRollbackHook
{
    public static final TxRollbackHook DEFAULT = new TxRollbackHook()
    {
        public void rollbackTransaction( int eventIdentifier )
        {
            // Do nothing from the ordinary here
        }
    };
    
    void rollbackTransaction( int eventIdentifier );
}
