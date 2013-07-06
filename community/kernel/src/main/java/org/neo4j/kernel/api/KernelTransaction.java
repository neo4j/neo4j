/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.StatementState;

/**
 * Represents a transaction of changes to the underlying graph.
 * Actual changes are made in the {@link #newStatementOperations() statements}
 * created from this transaction context. Changes made within a transaction
 * are visible to all operations within it.
 *
 * The reason for the separation between transactions and statements is isolation levels. While Neo4j is read-committed
 * isolation, a read can potentially involve multiple operations (think of a cypher statement). Within that read, or
 * statement if you will, the isolation level should be repeatable read, not read committed.
 *
 * Clearly separating between the concept of a transaction and the concept of a statement allows us to cater to this
 * type of isolation requirements.
 * 
 * TODO currently a {@link KernelTransaction} is used both for building the statement logic (once per db), as well as
 * being a transaction.
 */
public interface KernelTransaction
{
    /**
     * Creates a new {@link StatementOperations statement} which operations can be performed on.
     * When done it must be {@link StatementOperations#close() closed}.
     *
     * @return a new {@link StatementOperations} to do operations on.
     */
    StatementOperationParts newStatementOperations();

    StatementState newStatementState();
    
    // NOTE: The below methods don't yet do actual transaction work, that is still carried by
    //       the old TransactionImpl, WriteTransaction and friends.

    /**
     * Writes the changes this transaction wants to perform down to disk. If this method
     * returns successfully, the database guarantees that we can recover this transaction
     * after a crash.
     * <p/>
     * Normally, you should not use this, it is implicitly called by {@link #commit()}, but
     * it is a necessary thing if you are implementing two-phase commits.
     */
    void prepare();

    /**
     * Commit this transaction, this will make the changes in this context visible to other
     * transactions.
     * <p/>
     * If you have not called {@link #prepare()} before calling this method, the transaction
     * is implicitly prepared.
     */
    void commit() throws TransactionFailureException;

    /**
     * Roll back this transaction, undoing any changes that have been made.
     * @throws TransactionFailureException 
     */
    void rollback() throws TransactionFailureException;
}
