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
 * are visible to all operations within it. The changes are stored to the
 * graph upon {@link #finish() commit}, after marked as {@link #success() successful}.
 * 
 * Usage of a transaction context:
 * <ol>
 *   <li>get a {@link #newStatementContext() statement} and use it</li>
 *   <li>in a finally-block: close the statement context</li>
 *   <li>(do these above two an arbitrary number of times)</li>
 *   <li>call {@link #success()} if all statements were successful</li>
 *   <li>call {@link #finish()} to commit (if {@link #success()} has been called)
 *       or roll back (if {@link #success()} hasn't or {@link #failure()} has been called</li>
 * </ol>
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

    // TODO: It should be possible to implement distributed txs on top of this
    // replace with below methods with eg. .prepare(), .rollback() and .commit()

    /**
     * Marks this transaction as successful, so that a call to {@link #finish()} will
     * commit it. This call has no effect if {@link #failure()} has been called.
     * By default a transaction isn't marked as successful, so this must be called
     * in order to commit this transaction.
     */
    void success();
    
    /**
     * Marks this transaction as failed, so that a call to {@link #finalize()} will
     * roll it back. No calls to {@link #success()} will be able to mark this transaction
     * as successful after this call.
     */
    void failure();
    
    /**
     * Commits or rolls back this transaction depending on how {@link #success()}
     * and {@link #failure()} has been used.
     */
    void finish();
}
