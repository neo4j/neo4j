package org.neo4j.kernel;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;

public class TopLevelTransaction implements Transaction
{
    private boolean success = false;
    private final TransactionManager transactionManager;

    public TopLevelTransaction( TransactionManager transactionManager )
    {
        this.transactionManager = transactionManager;
    }

    public void failure()
    {
        this.success = false;
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
        success = true;
    }
    
    protected boolean isMarkedAsSuccessful()
    {
        try
        {
            return success && transactionManager.getTransaction().getStatus() !=
                    Status.STATUS_MARKED_ROLLBACK;
        }
        catch ( SystemException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    protected TransactionManager getTransactionManager()
    {
        return this.transactionManager;
    }
    
    public void finish()
    {
        try
        {
            if ( success )
            {
                if ( transactionManager.getTransaction() != null )
                {
                    transactionManager.getTransaction().commit();
                }
            }
            else
            {
                if ( transactionManager.getTransaction() != null )
                {
                    transactionManager.getTransaction().rollback();
                }
            }
        }
        catch ( RollbackException e )
        {
            throw new TransactionFailureException( "Unable to commit transaction", e );
        }
        catch ( Exception e )
        {
            if ( success )
            {
                throw new TransactionFailureException(
                    "Unable to commit transaction", e );
            }
            else
            {
                throw new TransactionFailureException(
                    "Unable to rollback transaction", e );
            }
        }
    }
}
