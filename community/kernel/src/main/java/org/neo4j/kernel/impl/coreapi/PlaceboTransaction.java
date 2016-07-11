/*
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
package org.neo4j.kernel.impl.coreapi;

import java.util.function.Supplier;

import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.AccessMode;

public class PlaceboTransaction implements InternalTransaction
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();
    private final Supplier<Statement> stmt;
    private final Supplier<KernelTransaction> currentTransaction;
    private boolean success;

    public PlaceboTransaction( Supplier<KernelTransaction> currentTransaction, Supplier<Statement> stmt )
    {
        this.stmt = stmt;
        this.currentTransaction = currentTransaction;
    }

    @Override
    public void terminate()
    {
        currentTransaction.get().markForTermination( Status.Transaction.Terminated );
    }

    @Override
    public void failure()
    {
        currentTransaction.get().failure();
    }

    @Override
    public void success()
    {
        success = true;
    }

    @Override
    public void close()
    {
        if ( !success )
        {
            currentTransaction.get().failure();
        }
    }

    @Override
    public Lock acquireWriteLock( PropertyContainer entity )
    {
        return locker.exclusiveLock( stmt, entity );
    }

    @Override
    public Lock acquireReadLock( PropertyContainer entity )
    {
        return locker.sharedLock( stmt, entity );
    }

    @Override
    public KernelTransaction.Type transactionType()
    {
        return currentTransaction.get().transactionType();
    }

    @Override
    public AccessMode mode()
    {
        return currentTransaction.get().mode();
    }

    @Override
    public KernelTransaction.Revertable restrict( AccessMode mode )
    {
        return currentTransaction.get().restrict( mode );
    }
}
