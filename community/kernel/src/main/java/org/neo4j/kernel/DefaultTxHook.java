package org.neo4j.kernel;

import org.neo4j.kernel.impl.transaction.TxHook;

class DefaultTxHook implements TxHook
{
    @Override
    public void initializeTransaction( int eventIdentifier )
    {
        // Do nothing from the ordinary here
    }

    public boolean hasAnyLocks( javax.transaction.Transaction tx )
    {
        return false;
    }

    public void finishTransaction( int eventIdentifier, boolean success )
    {
        // Do nothing from the ordinary here
    }

    @Override
    public boolean freeIdsDuringRollback()
    {
        return true;
    }
}
