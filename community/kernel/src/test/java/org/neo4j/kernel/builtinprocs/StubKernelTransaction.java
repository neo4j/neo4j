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
package org.neo4j.kernel.builtinprocs;

import java.util.Optional;

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.security.SecurityContext;

public class StubKernelTransaction implements KernelTransaction
{
    private final ReadOperations readOperations;

    StubKernelTransaction( ReadOperations readOperations )
    {
        this.readOperations = readOperations;
    }

    @Override
    public Statement acquireStatement()
    {
        return new StubStatement( readOperations );
    }

    @Override
    public void success()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void failure()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long closeTransaction() throws TransactionFailureException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean isOpen()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public SecurityContext securityContext()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Optional<Status> getReasonIfTerminated()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void markForTermination( Status reason )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long lastTransactionTimestampWhenStarted()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long lastTransactionIdWhenStarted()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long startTime()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long timeout()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void registerCloseListener( CloseListener listener )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Type transactionType()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long getTransactionId()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long getCommitTime()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Revertable overrideWith( SecurityContext context )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
