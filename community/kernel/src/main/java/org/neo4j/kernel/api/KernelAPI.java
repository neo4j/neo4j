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
 * The main API through which access to the Neo4j kernel is made, both read
 * and write operations are supported as well as transactions.
 * 
 * Changes to the graph (i.e. write operations) are performed via a
 * {@link #newTransactionContext() transaction context} where changes done
 * inside the transaction are visible in read operations for {@link StatementContext statements}
 * executed within that transaction context. Once {@link TransactionContext#finish() committed}
 * those changes are applied to the graph storage and made visible to all other transactions.
 * 
 * Read operations not associated with any particular transaction can be performed via
 * the {@link #newReadOnlyStatementContext() read-only statement context}.
 */
public interface KernelAPI
{
    /**
     * Creates and returns a new {@link TransactionContext} capable of modifying the
     * underlying graph. Changes made in it are visible within the transaction and can
     * be committed or rolled back.
     * 
     * @return a new {@link TransactionContext} for modifying the underlying graph.
     */
    TransactionContext newTransactionContext();

    /**
     * Returns a {@link StatementContext context} that can be used for read operations
     * that aren't associated with any specific transaction. Write operations on this
     * statement will throw exception.
     * 
     * @return a new {@link StatementContext} used for read operations not associated
     * with any transaction.
     */
    StatementContext newReadOnlyStatementContext();
}
