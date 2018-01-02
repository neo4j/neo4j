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
package org.neo4j.kernel.impl.transaction.log;


/**
 * A way to mark a transaction as committed after
 * {@link TransactionAppender#append(org.neo4j.kernel.impl.transaction.TransactionRepresentation, long) appended}
 * and manually {@link TransactionAppender#force() forced} and later closed after
 * {@link org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier#apply(
 * org.neo4j.kernel.impl.transaction.TransactionRepresentation, org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates,
 *  org.neo4j.kernel.impl.locking.LockGroup, long, org.neo4j.kernel.impl.api.TransactionApplicationMode)} .
 */
public interface Commitment
{
    /**
     * <p>
     *     Marks the transaction as committed and makes this fact public.
     * </p>
     */
    void publishAsCommitted();

    /**
     * <p>
     *     Marks the transaction as closed and makes this fact public.
     * </p>
     */
    void publishAsApplied();

    /**
     * @return the commitment transaction id
     */
    long transactionId();

    boolean markedAsCommitted();
}
