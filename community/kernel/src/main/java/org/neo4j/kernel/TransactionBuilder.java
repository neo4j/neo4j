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
package org.neo4j.kernel;

import org.neo4j.graphdb.Transaction;

/**
 * A builder for controlling certain behaviors of a transaction.
 * Methods that return {@link TransactionBuilder} returns a new instance
 * with the updated configuration that the method call modified.
 * When the behavior is as desired a call to {@link #begin()} will
 * begin the transaction and return a {@link Transaction}.
 *
 * <pre>
 * // To begin a transaction with default behavior
 * Transaction tx = graphDb.tx().begin();
 *
 * // To begin a transaction with relaxed force
 * Transaction tx = graphDb.tx().unforced().begin();
 *
 * // To have relaxed force optionally set by a condition
 * TransactionBuilder txBuilder = graphDb.tx();
 * if ( condition )
 * {
 *     txBuilder = txBuilder.unforced();
 * }
 * Transaction tx = txBuilder.begin();
 * </pre>
 *
 * @deprecated This will be moved to internal packages in the next major release.
 *
 * @author Mattias Persson
 */
@Deprecated
public interface TransactionBuilder
{
    /**
     * Starts a new transaction and associates it with the current thread.
     *
     * @return a new transaction instance
     */
    Transaction begin();
}
