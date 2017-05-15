/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.kernel.api.security.SecurityContext;

public class PlaceboTransaction implements InternalTransaction
{
    private static final PropertyContainerLocker locker = new PropertyContainerLocker();
    private final Supplier<Statement> stmt;
    private final KernelTransaction currentTransaction;
    private boolean success;

    public PlaceboTransaction( Supplier<KernelTransaction> currentTransaction, Supplier<Statement> stmt )
    {
        this.stmt = stmt;
        this.currentTransaction = currentTransaction.get();
    }

    @Override
    public void terminate()
    {
        currentTransaction.markForTermination( Status.Transaction.Terminated );
    }

    @Override
    public void failure()
    {
        currentTransaction.failure();
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
            currentTransaction.failure();
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
        return currentTransaction.transactionType();
    }

    @Override
    public SecurityContext securityContext()
    {
        return currentTransaction.securityContext();
    }

    @Override
    public KernelTransaction.Revertable overrideWith( SecurityContext context )
    {
        return currentTransaction.overrideWith( context );
    }

    @Override
    public Status terminationReason()
    {
        return currentTransaction.getReasonIfTerminated();
    }
}
