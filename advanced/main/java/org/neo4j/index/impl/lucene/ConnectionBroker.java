package org.neo4j.index.impl.lucene;

import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.impl.util.ArrayMap;

class ConnectionBroker
{
    private final ArrayMap<Transaction, LuceneXaConnection> txConnectionMap =
            new ArrayMap<Transaction, LuceneXaConnection>( 5, true, true );
    private final TransactionManager transactionManager;
    private final LuceneDataSource xaDs;

    ConnectionBroker( TransactionManager transactionManager,
            LuceneDataSource dataSource )
    {
        this.transactionManager = transactionManager;
        this.xaDs = dataSource;
    }

    LuceneXaConnection acquireResourceConnection()
    {
        LuceneXaConnection con = null;
        Transaction tx = this.getCurrentTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        con = txConnectionMap.get( tx );
        if ( con == null )
        {
            try
            {
                con = (LuceneXaConnection) xaDs.getXaConnection();
                if ( !tx.enlistResource( con.getXaResource() ) )
                {
                    throw new RuntimeException( "Unable to enlist '"
                                                + con.getXaResource() + "' in "
                                                + tx );
                }
                tx.registerSynchronization( new TxCommitHook( tx ) );
                txConnectionMap.put( tx, con );
            }
            catch ( javax.transaction.RollbackException re )
            {
                String msg = "The transaction is marked for rollback only.";
                throw new RuntimeException( msg, re );
            }
            catch ( javax.transaction.SystemException se )
            {
                String msg = "TM encountered an unexpected error condition.";
                throw new RuntimeException( msg, se );
            }
        }
        return con;
    }

    LuceneXaConnection acquireReadOnlyResourceConnection()
    {
        Transaction tx = this.getCurrentTransaction();
        return tx != null ? txConnectionMap.get( tx ) : null;
    }

    void releaseResourceConnectionsForTransaction( Transaction tx )
            throws NotInTransactionException
    {
        LuceneXaConnection con = txConnectionMap.remove( tx );
        if ( con != null )
        {
            con.destroy();
        }
    }

    void delistResourcesForTransaction() throws NotInTransactionException
    {
        Transaction tx = this.getCurrentTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        LuceneXaConnection con = txConnectionMap.get( tx );
        if ( con != null )
        {
            try
            {
                tx.delistResource( con.getXaResource(), XAResource.TMSUCCESS );
            }
            catch ( IllegalStateException e )
            {
                throw new RuntimeException(
                        "Unable to delist lucene resource from tx", e );
            }
            catch ( SystemException e )
            {
                throw new RuntimeException(
                        "Unable to delist lucene resource from tx", e );
            }
        }
    }

    private Transaction getCurrentTransaction()
            throws NotInTransactionException
    {
        try
        {
            return transactionManager.getTransaction();
        }
        catch ( SystemException se )
        {
            throw new NotInTransactionException(
                    "Error fetching transaction for current thread", se );
        }
    }

    private class TxCommitHook implements Synchronization
    {
        private final Transaction tx;

        TxCommitHook( Transaction tx )
        {
            this.tx = tx;
        }

        public void afterCompletion( int param )
        {
            releaseResourceConnectionsForTransaction( tx );
        }

        public void beforeCompletion()
        {
            delistResourcesForTransaction();
        }
    }
}
