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

import static org.neo4j.configuration.GraphDatabaseSettings.UNSPECIFIED_TIMEOUT;
import static org.neo4j.kernel.impl.util.ValueUtils.asParameterMapValue;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.connector.tx.TransactionOwner;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.bolt.tx.Transaction;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.TransactionType;
import org.neo4j.bolt.tx.error.TransactionException;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.HeapEstimator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.server.http.cypher.format.api.Statement;
import org.neo4j.server.http.cypher.format.api.TransactionUriScheme;

/**
 * Encapsulates executing statements in a transaction, committing the transaction, or rolling it back.
 *
 * Constructing a {@link TransactionHandle} does not immediately ask the kernel to create a
 * {@link org.neo4j.kernel.api.KernelTransaction}; instead a {@link org.neo4j.kernel.api.KernelTransaction} is
 * only created when the first statements need to be executed.
 *
 * At the end of each statement-executing method, the {@link org.neo4j.kernel.api.KernelTransaction} is either
 * suspended (ready to be resumed by a later operation), or committed, or rolled back.
 *
 * If you acquire instances of this class from {@link TransactionHandleRegistry}, it will prevent concurrent access to
 * the same instance. Therefore the implementation assumes that a single instance will only be accessed from
 * a single thread.
 *
 * All of the public methods on this class are "single-shot"; once you have called one method, the handle returns
 * itself
 * to the registry. If you want to use it again, you'll need to acquire it back from the registry to ensure exclusive
 * use.
 */
public class TransactionHandle implements TransactionTerminationHandle, TransactionOwner {
    public static final long SHALLOW_SIZE = HeapEstimator.shallowSizeOfInstance(TransactionHandle.class);

    private final String databaseName;
    private final TransactionRegistry registry;
    private final TransactionUriScheme uriScheme;
    private final TransactionManager transactionManager;
    private final Type type;
    private final Duration customTransactionTimeout;
    private final long id;
    private long expirationTimestamp = -1;
    private final InternalLogProvider userLogProvider;
    private final ClientConnectionInfo clientConnectionInfo;
    private final boolean readByDefault;

    private Transaction transaction;
    private LoginContext loginContext;
    MemoryTracker memoryTracker;
    AuthManager authManager;
    private final List<String> inputBookmarks;
    private String outputBookmark;

    TransactionHandle(
            String databaseName,
            TransactionRegistry registry,
            TransactionUriScheme uriScheme,
            boolean implicitTransaction,
            LoginContext loginContext,
            ClientConnectionInfo clientConnectionInfo,
            long customTransactionTimeoutMillis,
            TransactionManager transactionManager,
            InternalLogProvider logProvider,
            MemoryTracker memoryTracker,
            AuthManager authManager,
            boolean readByDefault,
            List<String> bookmarks) {
        this.databaseName = databaseName;
        this.registry = registry;
        this.uriScheme = uriScheme;
        this.type = implicitTransaction ? Type.IMPLICIT : Type.EXPLICIT;
        this.customTransactionTimeout = customTransactionTimeoutMillis != UNSPECIFIED_TIMEOUT
                ? Duration.ofMillis(customTransactionTimeoutMillis)
                : null;
        this.id = registry.begin(this);
        this.transactionManager = transactionManager;
        this.userLogProvider = logProvider;
        this.loginContext = loginContext;
        this.clientConnectionInfo = clientConnectionInfo;
        this.memoryTracker = memoryTracker;
        this.authManager = authManager;
        this.readByDefault = readByDefault;
        this.inputBookmarks = bookmarks;
    }

    URI uri() {
        return uriScheme.txUri(id);
    }

    boolean isImplicit() {
        return type == Type.IMPLICIT;
    }

    long getExpirationTimestamp() {
        return expirationTimestamp;
    }

    /**
     * this is the id of the transaction from the user's perspective.
     */
    long getId() {
        return id;
    }

    @Override
    public boolean terminate() {
        this.transaction.interrupt();
        return true;
    }

    void ensureActiveTransaction() throws TransactionException {
        if (this.transaction == null) {
            beginTransaction();
        }
    }

    org.neo4j.bolt.tx.statement.Statement executeStatement(Statement statement) throws TransactionException {
        return this.transaction.run(statement.getStatement(), asParameterMapValue(statement.getParameters()));
    }

    void forceRollback() throws TransactionException {
        this.transaction.rollback();
        transaction.close();
    }

    void suspendTransaction() {
        expirationTimestamp = registry.release(id, this);
    }

    void commit() throws TransactionException {
        try {
            outputBookmark = this.transaction.commit();
        } finally {
            registry.forget(id);
            this.transaction.close();
        }
    }

    void rollback() throws TransactionException {
        try {
            this.transaction.rollback();
        } finally {
            this.registry.forget(id);
            this.transaction.close();
        }
    }

    @Override
    public LoginContext loginContext() {
        return loginContext;
    }

    boolean hasTransactionContext() {
        return this.transaction != null;
    }

    @Override
    public MemoryTracker memoryTracker() {
        return this.memoryTracker;
    }

    @Override
    public ClientConnectionInfo info() {
        return this.clientConnectionInfo;
    }

    @Override
    public String selectedDefaultDatabase() {
        return this.databaseName;
    }

    public void beginTransaction() throws TransactionException {
        this.transaction = this.transactionManager.create(
                isImplicit() ? TransactionType.IMPLICIT : TransactionType.EXPLICIT,
                this,
                this.databaseName,
                readByDefault ? AccessMode.READ : AccessMode.WRITE,
                inputBookmarks,
                this.customTransactionTimeout,
                Collections.emptyMap(),
                null);
    }

    public TransactionManager transactionManager() {
        return transactionManager;
    }

    @Override
    public RoutingContext routingContext() {
        return new RoutingContext(true, Map.of());
    }

    public String getOutputBookmark() {
        return outputBookmark;
    }

    public void setOutputBookmark(String outputBookmark) {
        this.outputBookmark = outputBookmark;
    }
}
