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

import java.util.List;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;
import org.neo4j.time.SystemNanoClock;

/**
 * Transactional actions contains the business logic for executing statements against Neo4j across long-running
 * transactions.
 * <p>
 * The idiom for the public methods here is:
 * <p>
 * response.begin()
 * try {
 * // Do internal calls, saving errors into a common error list
 * } catch ( Neo4jError e )
 * {
 * errors.add(e);
 * } finally
 * {
 * response.finish(errors)
 * }
 * <p>
 * This is done to ensure we stick to the contract of the response handler, which is important, because if we skimp on
 * it, clients may be left waiting for results that never arrive.
 */
class TransactionFacade {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(TransactionFacade.class);

    private final String databaseName;
    private final TransactionRegistry registry;
    private final TransactionManager transactionManager;
    private final InternalLogProvider logProvider;
    private final AuthManager authManager;
    private final boolean readByDefault;

    TransactionFacade(
            String databaseName,
            TransactionRegistry registry,
            TransactionManager transactionManager,
            InternalLogProvider logProvider,
            AuthManager authManager,
            boolean readByDefault) {
        this.databaseName = databaseName;
        this.registry = registry;
        this.transactionManager = transactionManager;
        this.logProvider = logProvider;
        this.authManager = authManager;
        this.readByDefault = readByDefault;
    }

    TransactionHandle newTransactionHandle(
            TransactionUriScheme uriScheme,
            boolean implicitTransaction,
            LoginContext loginContext,
            ClientConnectionInfo clientConnectionInfo,
            MemoryTracker memoryTracker,
            long customTransactionTimeout,
            List<String> bookmarks) {
        memoryTracker.allocateHeap(TransactionHandle.SHALLOW_SIZE);

        return new TransactionHandle(
                databaseName,
                registry,
                uriScheme,
                implicitTransaction,
                loginContext,
                clientConnectionInfo,
                customTransactionTimeout,
                transactionManager,
                logProvider,
                memoryTracker,
                authManager,
                readByDefault,
                bookmarks);
    }

    TransactionHandle newTransactionHandle(
            TransactionUriScheme uriScheme,
            boolean implicitTransaction,
            LoginContext loginContext,
            ClientConnectionInfo clientConnectionInfo,
            MemoryTracker memoryTracker,
            long customTransactionTimeout,
            SystemNanoClock clock,
            boolean isReadOnlyTransaction,
            List<String> bookmarks) {
        memoryTracker.allocateHeap(TransactionHandle.SHALLOW_SIZE);

        return new TransactionHandle(
                databaseName,
                registry,
                uriScheme,
                implicitTransaction,
                loginContext,
                clientConnectionInfo,
                customTransactionTimeout,
                transactionManager,
                logProvider,
                memoryTracker,
                authManager,
                isReadOnlyTransaction,
                bookmarks);
    }

    TransactionHandle findTransactionHandle(long txId, LoginContext requestingUser)
            throws TransactionLifecycleException {
        return registry.acquire(txId, requestingUser);
    }

    TransactionHandle terminate(long txId, LoginContext loginContext) throws TransactionLifecycleException {
        return registry.terminate(txId, loginContext);
    }
}
