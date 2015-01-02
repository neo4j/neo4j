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
package org.neo4j.kernel.impl.api;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.Synchronization;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

public class NoOpJTATransaction implements Transaction
{
    @Override
    public void commit() throws HeuristicMixedException, HeuristicRollbackException, RollbackException, SecurityException, SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }

    @Override
    public boolean delistResource( XAResource xaResource, int i ) throws IllegalStateException, SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }

    @Override
    public boolean enlistResource( XAResource xaResource ) throws IllegalStateException, RollbackException,
            SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }

    @Override
    public int getStatus() throws SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }

    @Override
    public void registerSynchronization( Synchronization synchronization ) throws IllegalStateException, RollbackException, SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }

    @Override
    public void rollback() throws IllegalStateException, SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }

    @Override
    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        throw new UnsupportedOperationException( "Not allowed." );
    }
}
