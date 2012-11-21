/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.transaction.HeuristicMixedException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class ReadOnlyTxManager extends AbstractTransactionManager
        implements Lifecycle
{
    private static Logger log = Logger.getLogger( ReadOnlyTxManager.class.getName() );

    private ArrayMap<Thread, ReadOnlyTransactionImpl> txThreadMap;

    private int eventIdentifierCounter = 0;

    private XaDataSourceManager xaDsManager = null;

    public ReadOnlyTxManager( XaDataSourceManager xaDsManagerToUse )
    {
        xaDsManager = xaDsManagerToUse;
    }

    synchronized int getNextEventIdentifier()
    {
        return eventIdentifierCounter++;
    }

    @Override
    public void init()
    {
        txThreadMap = new ArrayMap<Thread, ReadOnlyTransactionImpl>( (byte) 5, true, true );
    }

    @Override
    public void start()
            throws Throwable
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public void shutdown()
            throws Throwable
    {
    }


    public void begin() throws NotSupportedException
    {
        Thread thread = Thread.currentThread();
        ReadOnlyTransactionImpl tx = txThreadMap.get( thread );
        if ( tx != null )
        {
            throw new NotSupportedException(
                    "Nested transactions not supported" );
        }
        tx = new ReadOnlyTransactionImpl( this );
        txThreadMap.put( thread, tx );
    }

    public void commit() throws RollbackException, HeuristicMixedException,
            IllegalStateException
    {
        Thread thread = Thread.currentThread();
        ReadOnlyTransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        if ( tx.getStatus() != Status.STATUS_ACTIVE
                && tx.getStatus() != Status.STATUS_MARKED_ROLLBACK )
        {
            throw new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) );
        }
        tx.doBeforeCompletion();
        if ( tx.getStatus() == Status.STATUS_ACTIVE )
        {
            commit( thread, tx );
        }
        else if ( tx.getStatus() == Status.STATUS_MARKED_ROLLBACK )
        {
            rollbackCommit( thread, tx );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) );
        }
    }

    private void commit( Thread thread, ReadOnlyTransactionImpl tx )
    {
        if ( tx.getResourceCount() == 0 )
        {
            tx.setStatus( Status.STATUS_COMMITTED );
        }
        else
        {
            throw new ReadOnlyDbException();
        }
        tx.doAfterCompletion();
        txThreadMap.remove( thread );
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
    }

    private void rollbackCommit( Thread thread, ReadOnlyTransactionImpl tx )
            throws HeuristicMixedException, RollbackException
    {
        try
        {
            tx.doRollback();
        }
        catch ( XAException e )
        {
            log.log( Level.SEVERE, "Unable to rollback marked transaction. "
                    + "Some resources may be commited others not. "
                    + "Neo4j kernel should be SHUTDOWN for "
                    + "resource maintance and transaction recovery ---->", e );
            throw Exceptions.withCause(
                    new HeuristicMixedException( "Unable to rollback " + " ---> error code for rollback: "
                            + e.errorCode ), e );
        }

        tx.doAfterCompletion();
        txThreadMap.remove( thread );
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
        throw new RollbackException(
                "Failed to commit, transaction rolledback" );
    }

    public void rollback() throws IllegalStateException, SystemException
    {
        Thread thread = Thread.currentThread();
        ReadOnlyTransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        if ( tx.getStatus() == Status.STATUS_ACTIVE ||
                tx.getStatus() == Status.STATUS_MARKED_ROLLBACK ||
                tx.getStatus() == Status.STATUS_PREPARING )
        {
            tx.doBeforeCompletion();
            try
            {
                tx.doRollback();
            }
            catch ( XAException e )
            {
                log.log( Level.SEVERE, "Unable to rollback marked or active transaction. "
                        + "Some resources may be commited others not. "
                        + "Neo4j kernel should be SHUTDOWN for "
                        + "resource maintance and transaction recovery ---->", e );
                throw Exceptions.withCause( new SystemException( "Unable to rollback "
                        + " ---> error code for rollback: " + e.errorCode ), e );
            }
            tx.doAfterCompletion();
            txThreadMap.remove( thread );
            tx.setStatus( Status.STATUS_NO_TRANSACTION );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) );
        }
    }

    public int getStatus()
    {
        Thread thread = Thread.currentThread();
        ReadOnlyTransactionImpl tx = txThreadMap.get( thread );
        if ( tx != null )
        {
            return tx.getStatus();
        }
        return Status.STATUS_NO_TRANSACTION;
    }

    public Transaction getTransaction()
    {
        return txThreadMap.get( Thread.currentThread() );
    }

    public void resume( Transaction tx ) throws IllegalStateException
    {
        Thread thread = Thread.currentThread();
        if ( txThreadMap.get( thread ) != null )
        {
            throw new IllegalStateException( "Transaction already associated" );
        }
        if ( tx != null )
        {
            ReadOnlyTransactionImpl txImpl = (ReadOnlyTransactionImpl) tx;
            if ( txImpl.getStatus() != Status.STATUS_NO_TRANSACTION )
            {
                txImpl.markAsActive();
                txThreadMap.put( thread, txImpl );
            }
        }
    }

    public Transaction suspend()
    {
        ReadOnlyTransactionImpl tx = txThreadMap.remove( Thread.currentThread() );
        if ( tx != null )
        {
            tx.markAsSuspended();
        }
        return tx;
    }

    public void setRollbackOnly() throws IllegalStateException
    {
        Thread thread = Thread.currentThread();
        ReadOnlyTransactionImpl tx = txThreadMap.get( thread );
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        tx.setRollbackOnly();
    }

    public void setTransactionTimeout( int seconds )
    {
    }

    byte[] getBranchId( XAResource xaRes )
    {
        if ( xaRes instanceof XaResource )
        {
            byte branchId[] = ((XaResource) xaRes).getBranchId();
            if ( branchId != null )
            {
                return branchId;
            }
        }
        return xaDsManager.getBranchId( xaRes );
    }

    String getTxStatusAsString( int status )
    {
        switch ( status )
        {
            case Status.STATUS_ACTIVE:
                return "STATUS_ACTIVE";
            case Status.STATUS_NO_TRANSACTION:
                return "STATUS_NO_TRANSACTION";
            case Status.STATUS_PREPARING:
                return "STATUS_PREPARING";
            case Status.STATUS_PREPARED:
                return "STATUS_PREPARED";
            case Status.STATUS_COMMITTING:
                return "STATUS_COMMITING";
            case Status.STATUS_COMMITTED:
                return "STATUS_COMMITED";
            case Status.STATUS_ROLLING_BACK:
                return "STATUS_ROLLING_BACK";
            case Status.STATUS_ROLLEDBACK:
                return "STATUS_ROLLEDBACK";
            case Status.STATUS_UNKNOWN:
                return "STATUS_UNKNOWN";
            case Status.STATUS_MARKED_ROLLBACK:
                return "STATUS_MARKED_ROLLBACK";
            default:
                return "STATUS_UNKNOWN(" + status + ")";
        }
    }

    public synchronized void dumpTransactions()
    {
        Iterator<ReadOnlyTransactionImpl> itr = txThreadMap.values().iterator();
        if ( !itr.hasNext() )
        {
            System.out.println( "No uncompleted transactions" );
            return;
        }
        System.out.println( "Uncompleted transactions found: " );
        while ( itr.hasNext() )
        {
            System.out.println( itr.next() );
        }
    }

    public int getEventIdentifier()
    {
        TransactionImpl tx = (TransactionImpl) getTransaction();
        if ( tx != null )
        {
            return tx.getEventIdentifier();
        }
        return -1;
    }
    
    @Override
    public void doRecovery() throws Throwable
    {
    }

    @Override
    public TransactionState getTransactionState()
    {
        return TransactionState.NO_STATE;
    }
}
