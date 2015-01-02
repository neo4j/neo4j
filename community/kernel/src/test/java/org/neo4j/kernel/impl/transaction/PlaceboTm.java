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
package org.neo4j.kernel.impl.transaction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.core.LockElement;
import org.neo4j.kernel.impl.core.NoTransactionState;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;

import static org.neo4j.helpers.Exceptions.launderedException;

public class PlaceboTm extends AbstractTransactionManager
{
    private LockManager lockManager;
    private final TxIdGenerator txIdGenerator;
    private final Transaction tx = new PlaceboTransaction();

    public PlaceboTm( LockManager lockManager, TxIdGenerator txIdGenerator )
    {
        this.lockManager = lockManager;
        this.txIdGenerator = txIdGenerator;
    }
    
    public void setLockManager( LockManager lockManager )
    {
        this.lockManager = lockManager;
    }
    
    @Override
    public void begin() throws NotSupportedException, SystemException
    {
    }

    @Override
    public void commit() throws RollbackException, HeuristicMixedException,
            HeuristicRollbackException, SecurityException, IllegalStateException,
            SystemException
    {
    }

    @Override
    public int getStatus() throws SystemException
    {
        return Status.STATUS_ACTIVE;
    }

    @Override
    public Transaction getTransaction() throws SystemException
    {
        return tx;
    }

    @Override
    public void resume( Transaction arg0 ) throws InvalidTransactionException,
            IllegalStateException, SystemException
    {
    }

    @Override
    public void rollback() throws IllegalStateException, SecurityException,
            SystemException
    {
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
    }

    @Override
    public void setTransactionTimeout( int arg0 ) throws SystemException
    {
    }

    @Override
    public Transaction suspend() throws SystemException
    {
        return null;
    }

    @Override
    public void init()
    {
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

    @Override
    public int getEventIdentifier()
    {
        return 0;
    }

    @Override
    public void doRecovery() throws Throwable
    {
    }

    @Override
    public TransactionState getTransactionState()
    {
        return new NoTransactionState()
        {
            @Override
            public LockElement acquireReadLock( Object resource )
            {
                try
                {
                    Transaction tx = getTransaction();
                    lockManager.getReadLock( resource, tx );
                    return new LockElement( resource, tx, LockType.READ, lockManager );
                }
                catch ( Exception e )
                {
                    throw launderedException( e );
                }
            }
            
            @Override
            public LockElement acquireWriteLock( Object resource )
            {
                try
                {
                    Transaction tx = getTransaction();
                    lockManager.getWriteLock( resource, tx );
                    return new LockElement( resource, tx, LockType.WRITE, lockManager );
                }
                catch ( SystemException e )
                {
                    throw launderedException( e );
                }
            }
            
            @Override
            public TxIdGenerator getTxIdGenerator()
            {
                return txIdGenerator;
            }
        };
    }
    
    private static class PlaceboTransaction implements Transaction
    {
        @Override
        public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException,
                SecurityException, SystemException
        {
        }

        @Override
        public boolean delistResource( XAResource xaRes, int flag ) throws IllegalStateException, SystemException
        {
            return true;
        }

        @Override
        public boolean enlistResource( XAResource xaRes ) throws IllegalStateException, RollbackException,
                SystemException
        {
            return true;
        }

        @Override
        public int getStatus() throws SystemException
        {
            return Status.STATUS_ACTIVE;
        }

        @Override
        public void registerSynchronization( Synchronization synch ) throws IllegalStateException, RollbackException,
                SystemException
        {
        }

        @Override
        public void rollback() throws IllegalStateException, SystemException
        {
        }

        @Override
        public void setRollbackOnly() throws IllegalStateException, SystemException
        {
        }
    }
}
