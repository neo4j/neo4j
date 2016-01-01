/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking.community;

import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.neo4j.kernel.impl.locking.Locks;

/**
 * A transaction used for the sole purpose of acquiring locks via the community lock manager. This exists solely to
 * allow using the community lock manager with the {@link Locks} API.
 */
public class LockTransaction implements Transaction
{
    private final static AtomicLong IDS = new AtomicLong( 0 );

    private final long id = IDS.getAndIncrement();

    static final Transaction NO_TRANSACTION = new LockTransaction();

    @Override
    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean delistResource( XAResource xaRes, int flag ) throws IllegalStateException, SystemException
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean enlistResource( XAResource xaRes ) throws IllegalStateException, RollbackException, SystemException
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getStatus() throws SystemException
    {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void registerSynchronization( Synchronization synch ) throws IllegalStateException, RollbackException,
            SystemException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String toString()
    {
        return String.format( "LockClient[%d]", id );
    }
}
