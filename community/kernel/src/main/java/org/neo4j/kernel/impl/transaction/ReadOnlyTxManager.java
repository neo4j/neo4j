/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import javax.transaction.HeuristicMixedException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.XaResource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.ThreadLocalWithSize;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class ReadOnlyTxManager extends AbstractTransactionManager
        implements Lifecycle
{
    private ThreadLocalWithSize<ReadOnlyTransactionImpl> txThreadMap;

    private int eventIdentifierCounter = 0;

    private XaDataSourceManager xaDsManager = null;
    private final StringLogger logger;
    private KernelAPI kernel;

    public ReadOnlyTxManager( XaDataSourceManager xaDsManagerToUse, StringLogger logger )
    {
        xaDsManager = xaDsManagerToUse;
        this.logger = logger;
    }

    synchronized int getNextEventIdentifier()
    {
        return eventIdentifierCounter++;
    }

    @Override
    public void init()
    {
    }

    @Override
    public void start()
            throws Throwable
    {
        txThreadMap = new ThreadLocalWithSize<ReadOnlyTransactionImpl>();
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


    @Override
    public void begin() throws NotSupportedException
    {
        if ( txThreadMap.get() != null )
        {
            throw new NotSupportedException(
                    "Nested transactions not supported" );
        }
        txThreadMap.set( new ReadOnlyTransactionImpl( this, logger ) );
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException,
            IllegalStateException
    {
        ReadOnlyTransactionImpl tx = txThreadMap.get();
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
            commit( tx );
        }
        else if ( tx.getStatus() == Status.STATUS_MARKED_ROLLBACK )
        {
            rollbackCommit( tx );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) );
        }
    }

    private void commit( ReadOnlyTransactionImpl tx )
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
        txThreadMap.remove();
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
    }

    private void rollbackCommit( ReadOnlyTransactionImpl tx )
            throws HeuristicMixedException, RollbackException
    {
        try
        {
            tx.doRollback();
        }
        catch ( XAException e )
        {
            logger.error( "Unable to rollback marked transaction. "
                    + "Some resources may be commited others not. "
                    + "Neo4j kernel should be SHUTDOWN for "
                    + "resource maintance and transaction recovery ---->", e );
            throw Exceptions.withCause(
                    new HeuristicMixedException( "Unable to rollback " + " ---> error code for rollback: "
                            + e.errorCode ), e );
        }

        tx.doAfterCompletion();
        txThreadMap.remove();
        tx.setStatus( Status.STATUS_NO_TRANSACTION );
        throw new RollbackException(
                "Failed to commit, transaction rolledback" );
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        ReadOnlyTransactionImpl tx = txThreadMap.get();
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
                logger.error("Unable to rollback marked or active transaction. "
                        + "Some resources may be commited others not. "
                        + "Neo4j kernel should be SHUTDOWN for "
                        + "resource maintance and transaction recovery ---->", e );
                throw Exceptions.withCause( new SystemException( "Unable to rollback "
                        + " ---> error code for rollback: " + e.errorCode ), e );
            }
            tx.doAfterCompletion();
            txThreadMap.remove();
            tx.setStatus( Status.STATUS_NO_TRANSACTION );
        }
        else
        {
            throw new IllegalStateException( "Tx status is: "
                    + getTxStatusAsString( tx.getStatus() ) );
        }
    }

    @Override
    public int getStatus()
    {
        ReadOnlyTransactionImpl tx = txThreadMap.get();
        if ( tx != null )
        {
            return tx.getStatus();
        }
        return Status.STATUS_NO_TRANSACTION;
    }

    @Override
    public Transaction getTransaction()
    {
        return txThreadMap.get();
    }

    @Override
    public void resume( Transaction tx ) throws IllegalStateException
    {
        if ( txThreadMap.get() != null )
        {
            throw new IllegalStateException( "Transaction already associated" );
        }
        if ( tx != null )
        {
            ReadOnlyTransactionImpl txImpl = (ReadOnlyTransactionImpl) tx;
            if ( txImpl.getStatus() != Status.STATUS_NO_TRANSACTION )
            {
                txImpl.markAsActive();
                txThreadMap.set( txImpl );
            }
        }
    }

    @Override
    public Transaction suspend()
    {
        ReadOnlyTransactionImpl tx = txThreadMap.get();
        txThreadMap.remove();
        if ( tx != null )
        {
            tx.markAsSuspended();
        }
        return tx;
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException
    {
        ReadOnlyTransactionImpl tx = txThreadMap.get();
        if ( tx == null )
        {
            throw new IllegalStateException( "Not in transaction" );
        }
        tx.setRollbackOnly();
    }

    @Override
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
    }

    @Override
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
    public StatementContext getStatementContext()
    {
        return kernel.newReadOnlyStatementContext();
    }

    @Override
    public void setKernel( KernelAPI kernel )
    {
        this.kernel = kernel;
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
