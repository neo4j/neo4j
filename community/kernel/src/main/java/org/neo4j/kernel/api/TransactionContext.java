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

/**
 * Represents a transaction of changes to the underlying graph.
 * Actual changes are made in the {@link #newStatementContext() statements}
 * created from this transaction context. Changes made within a transaction
 * are visible to all operations within it.
 */
public interface TransactionContext
{
    /**
     * Creates a new {@link StatementContext statement} which operations can be performed on.
     * When done it must be {@link StatementContext#close() closed}.
     *
     * @return a new {@link StatementContext} to do operations on.
     */
    StatementContext newStatementContext();

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
    void commit();

    /**
     * Roll back this transaction, undoing any changes that have been made.
     */
    void rollback();
}
