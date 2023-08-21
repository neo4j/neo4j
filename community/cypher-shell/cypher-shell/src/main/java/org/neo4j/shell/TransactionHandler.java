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
package org.neo4j.shell;

import java.util.Map;
import java.util.Optional;
import org.neo4j.driver.Value;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.state.BoltResult;

/**
 * An object capable of starting, committing, and rolling back transactions.
 */
public interface TransactionHandler {

    /**
     * Open a transaction for user submitted queries. Note, don't use for other purposes, like system queries.
     *
     * @throws CommandException if a new transaction could not be started
     */
    void beginTransaction() throws CommandException;

    /**
     * @throws CommandException if current transaction could not be committed
     */
    void commitTransaction() throws CommandException;

    /**
     * @throws CommandException if current transaction could not be rolled back
     */
    void rollbackTransaction() throws CommandException;

    /**
     * @return true if a transaction is currently open, false otherwise
     */
    boolean isTransactionOpen();

    /**
     * Run a cypher query directly submitted by the user.
     */
    Optional<BoltResult> runUserCypher(String cypher, Map<String, Value> queryParams) throws CommandException;

    Optional<BoltResult> runCypher(String cypher, Map<String, Value> queryParams, TransactionType type)
            throws CommandException;

    enum TransactionType {
        SYSTEM("system"),
        USER_DIRECT("user-direct"),
        USER_ACTION("user-action"),
        USER_TRANSPILED("user-transpiled");

        private final String value;

        TransactionType(String value) {
            this.value = value;
        }

        public String value() {
            return value;
        }
    }
}
