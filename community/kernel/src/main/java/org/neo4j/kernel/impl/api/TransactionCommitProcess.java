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

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;

/*
 * This interface represents the contract for committing a transaction. While the concept of a transaction is
 * captured in {@link TransactionRepresentation}, commit requires some more information to proceed, since a transaction
 * can come from various sources (normal commit, recovery etc) each of which can be committed but requires special
 * handling.
 *
 * A simple implementation of this would be to append to a log and then apply the
 * commands of the representation to the store that generated them. Another could
 * instead of appending to a log, write the transaction over the network to another
 * machine.
 */
public interface TransactionCommitProcess
{
    /**
     * The main work method.
     * @param representation The {@link TransactionRepresentation} to commit
     * @param locks the {@link LockGroup} to add locks to while committing the transaction. This lock group is expected
     *              to be managed by the caller - implementations should not attempt to close it.
     * @param commitEvent A tracer for the commit process
     * @param mode The {@link TransactionApplicationMode} to use
     * @return The transaction id assigned to the transaction as part of the commit
     * @throws TransactionFailureException If the commit process fails
     */
    long commit( TransactionRepresentation representation, LockGroup locks, CommitEvent commitEvent,
                 TransactionApplicationMode mode ) throws TransactionFailureException;
}
