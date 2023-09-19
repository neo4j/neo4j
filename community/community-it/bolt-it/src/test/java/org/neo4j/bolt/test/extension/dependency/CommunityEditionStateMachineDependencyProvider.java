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
package org.neo4j.bolt.test.extension.dependency;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.Mockito;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.basic.BasicAuthentication;
import org.neo4j.bolt.test.extension.db.ServerInstanceContext;
import org.neo4j.bolt.testing.extension.dependency.StateMachineDependencyProvider;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;
import org.neo4j.bolt.transport.Neo4jWithSocketSupportExtension;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.bolt.tx.TransactionManagerImpl;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.time.SystemNanoClock;

public class CommunityEditionStateMachineDependencyProvider implements StateMachineDependencyProvider {
    private final ServerInstanceContext instanceContext;

    private String defaultDatabase;
    private Authentication authentication;
    private TransactionManager transactionManager;

    public CommunityEditionStateMachineDependencyProvider(ExtensionContext context) {
        this(TestDatabaseManagementServiceBuilder.class, context);
    }

    protected CommunityEditionStateMachineDependencyProvider(
            Class<? extends TestDatabaseManagementServiceBuilder> defaultTestDatabaseManagementServiceBuilder,
            ExtensionContext context) {
        this.instanceContext = ServerInstanceContext.forExtensionContext(
                context, defaultTestDatabaseManagementServiceBuilder, Collections.emptyList(), Collections.emptyList());
    }

    private GraphDatabaseAPI getDatabaseAPI(ExtensionContext ctx) {
        var neo4j = Neo4jWithSocketSupportExtension.getInstance(ctx);
        return (GraphDatabaseAPI) neo4j.graphDatabaseService();
    }

    @Override
    public BoltGraphDatabaseManagementServiceSPI spi(ExtensionContext ctx) {
        var gdb = this.getDatabaseAPI(ctx);

        return gdb.getDependencyResolver().resolveDependency(BoltGraphDatabaseManagementServiceSPI.class);
    }

    @Override
    public SystemNanoClock clock(ExtensionContext ctx) {
        var gdb = this.getDatabaseAPI(ctx);

        return gdb.getDependencyResolver().resolveDependency(SystemNanoClock.class);
    }

    @Override
    public ConnectionHandle connection(ExtensionContext ctx) {
        return ConnectionMockFactory.newFactory("bolt-test")
                .withSelectedDefaultDatabase(defaultDatabase)
                .withAuthentication(authentication)
                .withTransactionManager(transactionManager)
                .withInterruptedCaptor(new AtomicInteger())
                .build();
    }

    @Override
    public Optional<Long> lastTransactionId(ExtensionContext ctx) {
        var gdb = this.getDatabaseAPI(ctx);

        return Optional.of(gdb.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getLastClosedTransactionId());
    }

    @Override
    public void init(ExtensionContext context, TestInfo testInfo) {
        var neo4j = this.instanceContext.configure(context);

        try {
            neo4j.init(testInfo);
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to initialize server", ex);
        }

        var gdb = (GraphDatabaseAPI) neo4j.graphDatabaseService();
        var dependencyResolver = gdb.getDependencyResolver();

        var spi = dependencyResolver.resolveDependency(BoltGraphDatabaseManagementServiceSPI.class);
        var clock = dependencyResolver.resolveDependency(SystemNanoClock.class);
        var authManager = dependencyResolver.resolveDependency(AuthManager.class);

        this.defaultDatabase =
                dependencyResolver.resolveDependency(Config.class).get(GraphDatabaseSettings.initial_default_database);
        this.authentication = new BasicAuthentication(authManager);
        this.transactionManager = new TransactionManagerImpl(spi, clock);
    }

    @Override
    public void close(ExtensionContext context) {
        this.instanceContext.stop(context);

        this.transactionManager = null;

        // FIXME: Mockito is currently suffering from a memory leak within inline mocks which
        //        requires us to clear all mocks at the end of execution in order to avoid OOMs
        Mockito.framework().clearInlineMocks();
    }
}
