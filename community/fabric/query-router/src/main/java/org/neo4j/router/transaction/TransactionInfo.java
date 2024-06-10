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
package org.neo4j.router.transaction;

import static java.util.Collections.emptyMap;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.fabric.transaction.StatementLifecycleTransactionInfo;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.NormalizedDatabaseName;
import org.neo4j.kernel.impl.query.QueryExecutionConfiguration;

/**
 * Container for all the information given by the bolt server for a given transaction
 */
public final class TransactionInfo {
    private final DatabaseReference sessionDatabase;
    private final KernelTransaction.Type type;
    private final LoginContext loginContext;
    private final ClientConnectionInfo clientInfo;
    private final List<String> bookmarks;
    private final Duration txTimeout;
    private final AccessMode accessMode;
    private Map<String, Object> txMetadata;
    private final RoutingContext routingContext;
    private final QueryExecutionConfiguration queryExecutionConfiguration;

    public TransactionInfo(
            DatabaseReference sessionDatabase,
            KernelTransaction.Type type,
            LoginContext loginContext,
            ClientConnectionInfo clientInfo,
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            RoutingContext routingContext,
            QueryExecutionConfiguration queryExecutionConfiguration) {
        this.type = type;
        this.loginContext = loginContext;
        this.clientInfo = clientInfo;
        this.bookmarks = bookmarks;
        this.txTimeout = txTimeout;
        this.accessMode = accessMode;
        this.txMetadata = txMetadata;
        this.routingContext = routingContext;
        this.queryExecutionConfiguration = queryExecutionConfiguration;
        this.sessionDatabase = sessionDatabase;
    }

    public TransactionInfo withDefaults(Config config) {
        return new TransactionInfo(
                sessionDatabase,
                type,
                loginContext,
                clientInfo,
                bookmarks,
                txTimeout != null ? txTimeout : config.get(GraphDatabaseSettings.transaction_timeout),
                accessMode,
                txMetadata != null ? txMetadata : emptyMap(),
                routingContext,
                queryExecutionConfiguration);
    }

    public StatementLifecycleTransactionInfo statementLifecycleTransactionInfo() {
        return new StatementLifecycleTransactionInfo(loginContext, clientInfo, txMetadata);
    }

    public void setTxMetadata(Map<String, Object> txMeta) {
        this.txMetadata = txMeta;
    }

    public NormalizedDatabaseName sessionDatabaseName() {
        return sessionDatabase.fullName();
    }

    public KernelTransaction.Type type() {
        return type;
    }

    public LoginContext loginContext() {
        return loginContext;
    }

    public ClientConnectionInfo clientInfo() {
        return clientInfo;
    }

    public List<String> bookmarks() {
        return bookmarks;
    }

    public Duration txTimeout() {
        return txTimeout;
    }

    public AccessMode accessMode() {
        return accessMode;
    }

    public Map<String, Object> txMetadata() {
        return txMetadata;
    }

    public RoutingContext routingContext() {
        return routingContext;
    }

    public QueryExecutionConfiguration queryExecutionConfiguration() {
        return queryExecutionConfiguration;
    }

    public boolean isComposite() {
        return sessionDatabase.isComposite();
    }

    public boolean targetsSystemDatabase() {
        return sessionDatabase.fullName().equals(new NormalizedDatabaseName(SYSTEM_DATABASE_NAME));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != this.getClass()) return false;
        var that = (TransactionInfo) obj;
        return Objects.equals(this.sessionDatabase.fullName(), that.sessionDatabase.fullName())
                && Objects.equals(this.type, that.type)
                && Objects.equals(this.loginContext, that.loginContext)
                && Objects.equals(this.clientInfo, that.clientInfo)
                && Objects.equals(this.bookmarks, that.bookmarks)
                && Objects.equals(this.txTimeout, that.txTimeout)
                && Objects.equals(this.accessMode, that.accessMode)
                && Objects.equals(this.txMetadata, that.txMetadata)
                && Objects.equals(this.routingContext, that.routingContext)
                && Objects.equals(this.queryExecutionConfiguration, that.queryExecutionConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                sessionDatabase.fullName(),
                type,
                loginContext,
                clientInfo,
                bookmarks,
                txTimeout,
                accessMode,
                txMetadata,
                routingContext,
                queryExecutionConfiguration);
    }

    @Override
    public String toString() {
        return "TransactionInfo[" + "sessionDatabaseName="
                + sessionDatabase.fullName() + ", " + "type="
                + type + ", " + "loginContext="
                + loginContext + ", " + "clientInfo="
                + clientInfo + ", " + "bookmarks="
                + bookmarks + ", " + "txTimeout="
                + txTimeout + ", " + "accessMode="
                + accessMode + ", " + "txMetadata="
                + txMetadata + ", " + "routingContext="
                + routingContext + ", " + "queryExecutionConfiguration="
                + queryExecutionConfiguration + ']';
    }
}
