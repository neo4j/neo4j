/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver;

/**
 * Represents a transaction in the Neo4j database.
 * <p>
 * This interface may seem surprising in that it does not have explicit "commit" or "rollback" methods.
 * It is designed to minimize the complexity of the code you need to write to use transactions in a safe way, ensuring
 * that transactions are properly rolled back even if there is an exception while the transaction is running.
 * <p>
 * <h2>Example:</h2>
 * <p>
 * <pre class="docTest:org.neo4j.driver.doctest.TransactionDocIT#classDoc">
 * {@code
 * try(Transaction tx = session.newTransaction() )
 * {
 * tx.run( "CREATE (n)" );
 * tx.success();
 * }
 * }
 * </pre>
 */
public interface Transaction extends AutoCloseable, StatementRunner
{
    /**
     * Mark this transaction as successful. You must call this method before calling {@link #close()} to have your
     * transaction committed.
     */
    void success();

    /**
     * Mark this transaction as failed. When you call {@link #close()}, the transaction will get rolled back.   q
     * <p>
     * After this method has been called, there is nothing that can be done to "un-mark" it. This is a safety feature
     * to make sure no other code calls {@link #success()} and makes a transaction commit that was meant to be rolled
     * back.
     * <p>
     * Example:
     * <p>
     * <pre class="docTest:org.neo4j.driver.doctest.TransactionDocIT#failure">
     * {@code
     * try(Transaction tx = session.newTransaction() )
     * {
     * tx.run( "CREATE (n)" );
     * tx.failure();
     * }
     * }
     * </pre>
     */
    void failure();

    @Override
    void close();
}
