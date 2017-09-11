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

import java.util.Optional;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.security.SecurityContext;

public interface InternalTransaction extends Transaction
{
    KernelTransaction.Type transactionType();

    SecurityContext securityContext();

    KernelTransaction.Revertable overrideWith( SecurityContext context );

    Optional<Status> terminationReason();

    /**
     * Set the isolation level of this transaction.
     * <p>
     * This must be done before the transaction is put to use, and can only be set once.
     *
     * @param isolationLevel The isolation level desired for this transaction.
     * @throws IllegalStateException if the given isolation level is not supported, if modification of isolation level
     * is not support in the given database configuration, if an isolation level has already been set on this
     * transaction, or if it is too late to modify the isolation level on this transaction e.g. if read or write
     * operations have already occurred in the transaction.
     */
    void setIsolationLevel( IsolationLevel isolationLevel );
}
