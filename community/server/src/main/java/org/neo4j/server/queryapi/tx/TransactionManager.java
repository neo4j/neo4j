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
package org.neo4j.server.queryapi.tx;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Session;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.util.VisibleForTesting;

public interface TransactionManager {

    Transaction begin(
            String txId,
            Session session,
            AuthToken authToken,
            String databaseName,
            TransactionConfig config,
            String txType)
            throws TransactionIdCollisionException;

    Transaction retrieveTransaction(String transactionId, String requestedDatabase, AuthToken accessingUser)
            throws TransactionConcurrentAccessException, TransactionNotFoundException, WrongUserException;

    /**
     * Releases the transaction allowing it to be picked up by another thread
     * @param txId
     */
    void releaseTransaction(String txId);

    /**
     * Removes the transaction and rolls back any state.
     * @param txId
     */
    void removeTransaction(String txId);

    void beginTimeoutJob();

    long openTransactionCount();

    /**
     * Close and remove all managed transactions.
     * This method is specially used in tests and it is not safe.
     */
    @VisibleForTesting
    void removeAllTransactions();
}
