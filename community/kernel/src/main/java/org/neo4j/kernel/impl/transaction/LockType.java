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

import javax.transaction.Transaction;

import org.neo4j.kernel.impl.core.LockElement;
import org.neo4j.kernel.impl.core.TransactionState;

/**
 * Enum defining the <CODE>READ</CODE> lock and the <CODE>WRITE</CODE> lock.
 */
public enum LockType
{
    READ
    {
        @Override
        public LockElement acquire( TransactionState state, Object resource )
        {
            return state.acquireReadLock( resource );
        }
        
        @Override
        public void release( LockManager lockManager, Object resource, Transaction tx )
        {
            lockManager.releaseReadLock( resource, tx );
        }
    },
    WRITE
    {
        @Override
        public LockElement acquire( TransactionState state, Object resource )
        {
            return state.acquireWriteLock( resource );
        }
        
        @Override
        public void release( LockManager lockManager, Object resource, Transaction tx )
        {
            lockManager.releaseWriteLock( resource, tx );
        }
    };
    
    public abstract LockElement acquire( TransactionState state, Object resource );
    
    public abstract void release( LockManager lockManager, Object resource, Transaction tx );
}
