/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.tx;

import java.util.Optional;
import org.neo4j.bolt.tx.error.TransactionCloseException;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.bolt.tx.error.statement.StatementException;
import org.neo4j.bolt.tx.statement.Statement;
import org.neo4j.values.virtual.MapValue;

public interface Transaction {

    /**
     * Retrieves the globally unique identifier which has been allocated to this transaction.
     *
     * @return a transaction identifier.
     */
    String id();

    /**
     * Retrieves the type of this transaction as specified at creation time.
     *
     * @return a transaction type.
     */
    TransactionType type();

    /**
     * Evaluates whether this transaction remains open (e.g. has not been committed or rolled back).
     *
     * @return true if open, false otherwise.
     */
    boolean isOpen();

    /**
     * Evaluates whether this transaction still holds a valid transaction handle (e.g. has not been
     * closed yet).
     *
     * @return true if valid, false otherwise.
     */
    boolean isValid();

    /**
     * Retrieves the identifier of the latest statement to be created within the transaction.
     *
     * @return a statement identifier.
     */
    long latestStatementId();

    /**
     * Evaluates whether there is at least one statement which has yet to be fully consumed within
     * this transaction.
     *
     * @return true if at least one statement remains, false otherwise.
     */
    boolean hasOpenStatement();

    /**
     * Identifies whether this transaction has been terminated as a result of a failure during
     * query execution.
     *
     * @return true if failed, false otherwise.
     */
    boolean hasFailed();

    /**
     * Executes a statement within the context of this transaction.
     *
     * @param statement a cypher statement.
     * @param params a set of parameters to pass to the runtime.
     * @return a collection of statement metadata.
     */
    Statement run(String statement, MapValue params) throws StatementException;

    /**
     * Retrieves a reference to a given statement within this transaction or an empty optional when
     * no such statement exists.
     * <p />
     * <em>Note:</em> When a statement is fully consumed, it will be automatically removed from the
     * transaction.
     *
     * @param id a statement id.
     * @return a statement or an empty optional.
     */
    Optional<Statement> getStatement(long id);

    /**
     * Commits the transaction in its current state and returns a bookmark which refers to the applied
     * changes.
     *
     * @return a bookmark.
     */
    String commit() throws TransactionException;

    /**
     * Discards the state of this transaction.
     */
    void rollback() throws TransactionException;

    /**
     * Attempts to interrupt the transaction and all running child statements.
     */
    void interrupt();

    /**
     * Validates the current state of the transaction.
     * <p />
     * When the transaction has been terminated through an external source (such as an
     * administrative command or a timeout), a termination status code will be returned.
     * <p />
     * If the transaction remains valid, an empty optional is returned instead.
     *
     * @return a termination status code or an empty optional
     */
    boolean validate();

    void close() throws TransactionCloseException;

    void registerListener(Listener listener);

    void removeListener(Listener listener);

    interface Listener {

        default void onCommit(Transaction transaction, String bookmark) {}

        default void onRollback(Transaction transaction) {}

        default void onClose(Transaction transaction) {}
    }
}
