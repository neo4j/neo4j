/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.storageengine.api.TransactionApplicationMode;

/**
 * This interface represents the contract for committing a batch of transactions. While the concept of a transaction is
 * captured in {@link TransactionRepresentation}, commit requires some more information to proceed, since a transaction
 * can come from various sources (normal commit, recovery etc) each of which can be committed but requires
 * different/additional handling.
 *
 * A simple implementation of this would be to append to a log and then apply the commands of the representation
 * to storage that generated them. Another could instead serialize the transactions over the network to another machine.
 */
public interface TransactionCommitProcess
{
    /**
     * Commit a batch of transactions. After this method returns the batch of transaction should be committed
     * durably and be recoverable in the event of failure after this point.
     *
     * @param batch transactions to commit.
     * @param commitEvent {@link CommitEvent} for traceability.
     * @param mode The {@link TransactionApplicationMode} to use when applying these transactions.
     * @return transaction id of the last committed transaction in this batch.
     * @throws TransactionFailureException If the commit process fails.
     */
    long commit( TransactionToApply batch, CommitEvent commitEvent, TransactionApplicationMode mode )
            throws TransactionFailureException;
}
