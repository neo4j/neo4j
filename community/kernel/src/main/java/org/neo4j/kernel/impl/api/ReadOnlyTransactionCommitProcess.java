/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/**
 * For databases in read_only mode, the implementation of {@link org.neo4j.kernel.impl.api.TransactionCommitProcess}
 * will simply always throw an exception on commit, to ensure that no changes are made.
 */
public class ReadOnlyTransactionCommitProcess implements TransactionCommitProcess
{
    @Override
    public long commit( TransactionRepresentation representation, LockGroup locks, CommitEvent commitEvent,
                        TransactionApplicationMode mode ) throws TransactionFailureException
    {
        throw new ReadOnlyDbException();
    }
}
