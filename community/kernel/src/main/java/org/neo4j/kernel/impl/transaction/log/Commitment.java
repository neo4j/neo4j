/*
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
package org.neo4j.kernel.impl.transaction.log;

/**
 * A way to mark a transaction as committed after
 * {@link TransactionAppender#append(org.neo4j.kernel.impl.transaction.TransactionRepresentation, long) appended}
 * and manually {@link TransactionAppender#force() forced}.
 */
public interface Commitment
{
    /**
     * <p>
     *     Marks the transaction as committed and makes this fact public.
     * </p>
     * <p>
     *     After this call the caller must see to that the transaction gets properly closed as well, i.e
     *     {@link TransactionIdStore#transactionClosed(long)}.
     * </p>
     */
    void publishAsCommitted();
}
