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
package org.neo4j.server.http.cypher;

import org.neo4j.internal.kernel.api.security.LoginContext;

/**
 * Stores transaction contexts for the server, including handling concurrency safe ways to acquire
 * transaction contexts back, as well as timing out and closing transaction contexts that have been
 * left unused.
 */
public interface TransactionRegistry {
    long begin(TransactionHandle handle);

    long release(long id, TransactionHandle transactionHandle);

    TransactionHandle acquire(long id) throws TransactionLifecycleException;

    void forget(long id);

    TransactionHandle terminate(long id) throws TransactionLifecycleException;

    void rollbackAllSuspendedTransactions();

    void rollbackSuspendedTransactionsIdleSince(long oldestLastActiveTime);

    default TransactionHandle acquire(long id, LoginContext requestingUser) throws TransactionLifecycleException {
        assertSameUser(getLoginContextForTransaction(id), requestingUser);
        return acquire(id);
    }

    default TransactionHandle terminate(long id, LoginContext requestingUser) throws TransactionLifecycleException {
        assertSameUser(getLoginContextForTransaction(id), requestingUser);
        return terminate(id);
    }

    default void assertSameUser(LoginContext owningUser, LoginContext requestingUser) throws InvalidTransactionId {
        if (!owningUser
                .subject()
                .authenticatedUser()
                .equals(requestingUser.subject().authenticatedUser())) {
            throw new InvalidTransactionId();
        }
    }

    LoginContext getLoginContextForTransaction(long id) throws InvalidTransactionId;
}
