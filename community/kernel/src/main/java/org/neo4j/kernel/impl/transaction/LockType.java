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

import javax.transaction.Transaction;

import org.neo4j.kernel.impl.core.TransactionState;

/**
 * Enum defining the <CODE>READ</CODE> lock and the <CODE>WRITE</CODE> lock.
 */
public enum LockType
{
    READ
    {
        @Override
        public void acquire( Object resource, LockManager lockManager, Transaction tx )
        {
            lockManager.getReadLock( resource, tx );
        }

        @Override
        public void unacquire( Object resource, LockManager lockManager, TransactionState state, Transaction tx )
        {
            lockManager.releaseReadLock( resource, tx );
        }

        @Override
        public void release( Object resource, LockManager lockManager, Transaction tx )
        {
            lockManager.releaseReadLock( resource, tx );
        }
    },
    WRITE
    {
        @Override
        public void acquire( Object resource, LockManager lockManager, Transaction tx )
        {
            lockManager.getWriteLock( resource, tx );
        }

        @Override
        public void unacquire( Object resource, LockManager lockManager, TransactionState state, Transaction tx )
        {
            state.addLockToTransaction( lockManager, resource, this );
        }

        @Override
        public void release( Object resource, LockManager lockManager, Transaction tx )
        {
            lockManager.releaseWriteLock( resource, tx );
        }
    };
    
    public abstract void acquire( Object resource, LockManager lockManager, Transaction tx );

    public abstract void unacquire( Object resource, LockManager lockManager, TransactionState state, Transaction tx );
    
    public abstract void release( Object resource, LockManager lockManager, Transaction tx );
    
    // Below methods are shortcuts, passing Transaction as null.
    // They expect code further down the stack to pick up the current
    // transaction from the transaction manager.
    // TODO: Perhaps we can figure out the current TX here directly instead?
    
    public void acquire( Object resource, LockManager lockManager)
    {
        acquire( resource, lockManager, null);
    }
    
    public void unacquire( Object resource, LockManager lockManager, TransactionState state )
    {
        unacquire( resource, lockManager, state, null);
    }
    
    public void release( Object resource, LockManager lockManager ) 
    {
        release( resource, lockManager, null);
    }
}