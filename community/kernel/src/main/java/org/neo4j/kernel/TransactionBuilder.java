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
    
    /**
     * Relaxes the forcing constraint of the logical logs for this transaction
     * so that the data is written out, but not forced to disk.
     * 
     * Pros
     * <ul>
     * <li>The overhead of committing a transaction is lowered due to the removal
     * of the force, i.e. I/O waiting for the disk to actually write the bytes down.
     * The smaller the transaction the bigger percentage of it is spent forcing
     * the logical logs to disk, so small transactions get most benefit from
     * being unforced.</li>
     * </ul>
     * 
     * Cons
     * <ul>
     * <li>Since there's no guarantee that the data is actually written down to disk
     * the data provided in this transaction could be lost in the event of power failure
     * or similar. It is however guaranteed that the data that has been written to disk
     * is consistent so the worst case scenario for unforced transactions is that a couple
     * of seconds (depending on write load) worth of data might be lost in an event of
     * power failure, but the database will still get back to a consistent state after
     * a recovery.</li>
     * </ul>
     * 
     * @return a {@link TransactionBuilder} instance with relaxed force set.
     */
    TransactionBuilder unforced();
}
