/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.runtime;

import static java.time.Duration.ofSeconds;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.SocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.dbapi.CustomBookmarkFormatParser;
import org.neo4j.bolt.dbapi.impl.BoltKernelDatabaseManagementServiceProvider;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.BoltProtocolRegistry;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.bolt.protocol.common.connector.connection.authentication.AuthenticationFlag;
import org.neo4j.bolt.protocol.common.fsm.StateMachine;
import org.neo4j.bolt.protocol.v40.BoltProtocolV40;
import org.neo4j.bolt.protocol.v40.bookmark.BookmarkParserV40;
import org.neo4j.bolt.protocol.v41.BoltProtocolV41;
import org.neo4j.bolt.protocol.v43.BoltProtocolV43;
import org.neo4j.bolt.protocol.v44.BoltProtocolV44;
import org.neo4j.bolt.protocol.v51.BoltProtocolV51;
import org.neo4j.bolt.security.Authentication;
import org.neo4j.bolt.security.basic.BasicAuthentication;
import org.neo4j.bolt.security.error.AuthenticationException;
import org.neo4j.bolt.transaction.StatementProcessorTxManager;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseContextProvider;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.LogService;
import org.neo4j.monitoring.Monitors;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.time.Clocks;

public class SessionExtension implements BeforeEachCallback, AfterEachCallback {
    private final Supplier<TestDatabaseManagementServiceBuilder> builderFactory;

    private GraphDatabaseAPI gdb;
    private BoltProtocolRegistry protocolRegistry;
    private DatabaseManagementService managementService;
    private Authentication authentication;

    private final List<StateMachine> runningMachines = new ArrayList<>();
    private boolean authEnabled;

    public SessionExtension() {
        this(TestDatabaseManagementServiceBuilder::new);
    }

    public SessionExtension(Supplier<TestDatabaseManagementServiceBuilder> builderFactory) {
        this.builderFactory = builderFactory;
    }

    public StateMachine newMachine(ProtocolVersion version) {
        assertTestStarted();

        var protocol = protocolRegistry
                .get(version)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported protocol version: " + version));

        var connection = this.createConnection();

        var stateMachine = protocol.createStateMachine(connection);
        runningMachines.add(stateMachine);
        return stateMachine;
    }

    private Connection createConnection() {
        var connection = mock(Connection.class, RETURNS_MOCKS);
        when(connection.id()).thenReturn("bolt-test");
        when(connection.selectedDefaultDatabase()).thenAnswer(invocation -> this.defaultDatabaseName());

        var interruptCounter = new AtomicInteger();
        doAnswer(invocation -> {
                    interruptCounter.incrementAndGet();
                    return null; // void function
                })
                .when(connection)
                .interrupt();
        doAnswer(invocation -> {
                    int counter;
                    do {
                        counter = interruptCounter.get();
                        if (counter == 0) {
                            return true;
                        }
                    } while (interruptCounter.compareAndSet(counter, counter - 1));

                    return counter <= 1;
                })
                .when(connection)
                .reset();
        when(connection.isInterrupted()).thenAnswer(invocation -> interruptCounter.get() != 0);

        // TODO: Migrate this functionality to ConnectionMockFactory
        var loginContext = new AtomicReference<LoginContext>();
        when(connection.loginContext()).thenAnswer(invocation -> loginContext.get());
        try {
            when(connection.logon(any())).thenAnswer(invocation -> {
                var result = authentication.authenticate(
                        invocation.getArgument(0),
                        new BoltConnectionInfo(
                                "bolt-test", "bolt-test", mock(SocketAddress.class), mock(SocketAddress.class)));
                loginContext.set(result.getLoginContext());

                if (result.credentialsExpired()) {
                    return AuthenticationFlag.CREDENTIALS_EXPIRED;
                }

                return null;
            });
        } catch (AuthenticationException ignore) {
        }

        return connection;
    }

    public DatabaseManagementService managementService() {
        assertTestStarted();
        return managementService;
    }

    public String defaultDatabaseName() {
        assertTestStarted();
        DependencyResolver resolver = gdb.getDependencyResolver();
        Config config = resolver.resolveDependency(Config.class);
        return config.get(GraphDatabaseSettings.initial_default_database);
    }

    public DatabaseIdRepository databaseIdRepository() {
        assertTestStarted();
        var resolver = gdb.getDependencyResolver();
        var databaseManager = resolver.resolveDependency(DatabaseContextProvider.class);
        return databaseManager.databaseIdRepository();
    }

    @Override
    public void beforeEach(ExtensionContext extensionContext) {
        managementService = builderFactory
                .get()
                .impermanent()
                .setConfig(GraphDatabaseSettings.auth_enabled, authEnabled)
                .build();
        gdb = (GraphDatabaseAPI) managementService.database(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);

        var resolver = gdb.getDependencyResolver();
        var config = resolver.resolveDependency(Config.class);
        var logService = resolver.resolveDependency(LogService.class);
        var databaseContextProvider = resolver.resolveDependency(DatabaseContextProvider.class);

        var clock = Clocks.nanoClock();
        var txManager = new StatementProcessorTxManager();
        var defaultDatabaseResolver = new CommunityDefaultDatabaseResolver(
                config, () -> managementService.database(GraphDatabaseSettings.SYSTEM_DATABASE_NAME));
        var databaseManagementService = new BoltKernelDatabaseManagementServiceProvider(
                managementService, new Monitors(), clock, ofSeconds(30));

        var bookmarksParser = new BookmarkParserV40(
                databaseContextProvider.databaseIdRepository(), CustomBookmarkFormatParser.DEFAULT);

        protocolRegistry = BoltProtocolRegistry.builder()
                .register(new BoltProtocolV40(
                        logService, databaseManagementService, defaultDatabaseResolver, txManager, clock))
                .register(new BoltProtocolV41(
                        logService, databaseManagementService, defaultDatabaseResolver, txManager, clock))
                .register(new BoltProtocolV43(
                        logService, databaseManagementService, defaultDatabaseResolver, txManager, clock))
                .register(new BoltProtocolV44(
                        logService, databaseManagementService, defaultDatabaseResolver, txManager, clock))
                .register(new BoltProtocolV51(
                        logService, databaseManagementService, defaultDatabaseResolver, txManager, clock))
                .build();

        var authManager = resolver.resolveDependency(AuthManager.class);
        authentication = new BasicAuthentication(authManager);
    }

    @Override
    public void afterEach(ExtensionContext extensionContext) {
        try {
            IOUtils.closeAll(runningMachines);
            runningMachines.clear();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        managementService.shutdown();
    }

    private void assertTestStarted() {
        if (protocolRegistry == null || gdb == null) {
            throw new IllegalStateException("Cannot access test environment before test is running.");
        }
    }

    private static Authentication authentication(AuthManager authManager) {
        return new BasicAuthentication(authManager);
    }

    public long lastClosedTxId() {
        return gdb.getDependencyResolver()
                .resolveDependency(TransactionIdStore.class)
                .getLastClosedTransactionId();
    }

    public static URL putTmpFile(String prefix, String suffix, String contents) throws IOException {
        Path tempFile = Files.createTempFile(prefix, suffix);
        tempFile.toFile().deleteOnExit();
        try (PrintWriter out = new PrintWriter(Files.newOutputStream(tempFile), false, StandardCharsets.UTF_8)) {
            out.println(contents);
        }
        return tempFile.toUri().toURL();
    }

    public SessionExtension withAuthEnabled(boolean authEnabled) {
        this.authEnabled = authEnabled;
        return this;
    }
}
