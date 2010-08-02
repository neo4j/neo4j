package org.neo4j.kernel;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;

public class PlaceboTransaction implements Transaction
{
    private final TransactionManager transactionManager;

    public PlaceboTransaction( TransactionManager transactionManager )
    {
        // we should override all so null is ok
        this.transactionManager = transactionManager;
    }

    public void failure()
    {
        try
        {
            transactionManager.getTransaction().setRollbackOnly();
        }
        catch ( Exception e )
        {
            throw new TransactionFailureException(
                "Failed to mark transaction as rollback only.", e );
        }
    }

    public void success()
    {
    }

    public void finish()
    {
    }
}
