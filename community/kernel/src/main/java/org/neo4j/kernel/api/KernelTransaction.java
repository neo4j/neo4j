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
package org.neo4j.kernel.api;

/**
 * Represents a transaction of changes to the underlying graph.
 * Actual changes are made in the {@linkplain #acquireStatement() statements} acquired from this transaction.
 * Changes made within a transaction are visible to all operations within it.
 *
 * The reason for the separation between transactions and statements is isolation levels. While Neo4j is read-committed
 * isolation, a read can potentially involve multiple operations (think of a cypher statement). Within that read, or
 * statement if you will, the isolation level should be repeatable read, not read committed.
 *
 * Clearly separating between the concept of a transaction and the concept of a statement allows us to cater to this
 * type of isolation requirements.
 *
 * This class has a 1-1 relationship with{@link org.neo4j.kernel.impl.nioneo.xa.NeoStoreTransaction}, please see its'
 * javadoc for details.
 */
public interface KernelTransaction
{
    Statement acquireStatement();

    // Made unavailable for now, should be re-instated once the WriteTransaction/KernelTransaction structure is
    // sorted out.
//    void prepare();
//
//    /**
//     * Commit this transaction, this will make the changes in this context visible to other
//     * transactions.
//     */
//    void commit() throws TransactionFailureException;
//
//    /** Roll back this transaction, undoing any changes that have been made. */
//    void rollback() throws TransactionFailureException;
}
