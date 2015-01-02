/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index;

import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.impl.transaction.xaframework.XaConnection;
import org.neo4j.kernel.impl.util.ArrayMap;

public abstract class IndexConnectionBroker<T extends XaConnection>
{
    private final ArrayMap<Transaction, T> txConnectionMap =
            new ArrayMap<Transaction, T>( (byte)5, true, true );
    private final TransactionManager transactionManager;

    protected IndexConnectionBroker( TransactionManager transactionManager )
    {
        this.transactionManager = transactionManager;
    }

    public T acquireResourceConnection()
    {
        Transaction tx = this.getCurrentTransaction();
        if ( tx == null )
        {
            throw new NotInTransactionException();
        }
        T con = txConnectionMap.get( tx );
        if ( con == null )
        {
            try
            {
                con = newConnection();
                if ( !con.enlistResource( tx ) )
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
            catch ( SystemException se )
            {
                String msg = "TM encountered an unexpected error condition.";
                throw new RuntimeException( msg, se );
            }
        }
        return con;
    }
    
    protected abstract T newConnection();

    public T acquireReadOnlyResourceConnection()
    {
        Transaction tx = this.getCurrentTransaction();
        return tx != null ? txConnectionMap.get( tx ) : null;
    }

    void releaseResourceConnectionsForTransaction( Transaction tx )
            throws NotInTransactionException
    {
        T con = txConnectionMap.remove( tx );
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
        T con = txConnectionMap.get( tx );
        if ( con != null )
        {
            try
            {
                con.delistResource(tx, XAResource.TMSUCCESS);
            }
            catch ( IllegalStateException | SystemException e )
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
