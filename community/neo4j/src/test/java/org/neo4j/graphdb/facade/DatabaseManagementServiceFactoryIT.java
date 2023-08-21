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
package org.neo4j.graphdb.facade;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_logical_logs;

import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpsConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.server.CommunityNeoWebServer;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.web.DisabledNeoWebServer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DatabaseManagementServiceFactoryIT {
    @Inject
    private TestDirectory testDirectory;

    private DatabaseManagementService managementService;

    @Nested
    class ManagementServiceIT {
        @BeforeEach
        void setUp() {
            managementService = getDatabaseManagementService();
        }

        @AfterEach
        void tearDown() {
            if (managementService != null) {
                managementService.shutdown();
            }
        }

        @Test
        void reportCorrectDatabaseNames() {
            GraphDatabaseService system = managementService.database(SYSTEM_DATABASE_NAME);
            GraphDatabaseService neo4j = managementService.database(DEFAULT_DATABASE_NAME);
            assertEquals(SYSTEM_DATABASE_NAME, system.databaseName());
            assertEquals(DEFAULT_DATABASE_NAME, neo4j.databaseName());
        }

        @Test
        void haveTwoDatabasesByDefault() {
            assertEquals(2, managementService.listDatabases().size());
        }

        @Test
        void failToCreateNonDefaultDatabase() {
            assertThrows(DatabaseManagementException.class, () -> managementService.createDatabase("newDb"));
        }

        @Test
        void failToDropDatabase() {
            for (String databaseName : managementService.listDatabases()) {
                assertThrows(DatabaseManagementException.class, () -> managementService.dropDatabase(databaseName));
            }
        }

        @Test
        void failToStartStopSystemDatabase() {
            assertThrows(
                    DatabaseManagementException.class, () -> managementService.shutdownDatabase(SYSTEM_DATABASE_NAME));
            assertThrows(
                    DatabaseManagementException.class, () -> managementService.startDatabase(SYSTEM_DATABASE_NAME));
        }

        @Test
        void shutdownShouldShutdownAllDatabases() {
            ShutdownListenerDatabaseEventListener shutdownListenerDatabaseEventListener =
                    new ShutdownListenerDatabaseEventListener();
            managementService.registerDatabaseEventListener(shutdownListenerDatabaseEventListener);
            managementService.shutdown();
            managementService = null;

            assertEquals(2, shutdownListenerDatabaseEventListener.getShutdownInvocations());
        }

        private DatabaseManagementService getDatabaseManagementService() {
            DatabaseManagementServiceFactory databaseManagementServiceFactory =
                    new DatabaseManagementServiceFactory(DbmsInfo.COMMUNITY, CommunityEditionModule::new);
            Config cfg = Config.newBuilder()
                    .set(neo4j_home, testDirectory.absolutePath())
                    .set(preallocate_logical_logs, false)
                    .build();
            return databaseManagementServiceFactory.build(cfg, false, GraphDatabaseDependencies.newDependencies());
        }
    }

    private static Stream<Arguments> serverSettings() {
        return Stream.of(
                arguments(Map.of(), DisabledNeoWebServer.class, null, null),
                arguments(
                        Map.of(
                                HttpConnector.enabled,
                                true,
                                HttpConnector.listen_address,
                                new SocketAddress("localhost", 0)),
                        CommunityNeoWebServer.class,
                        null,
                        null),
                arguments(
                        Map.of(
                                HttpConnector.enabled,
                                true,
                                HttpConnector.listen_address,
                                new SocketAddress("localhost", 0),
                                ServerSettings.http_enabled_modules,
                                Set.of()),
                        DisabledNeoWebServer.class,
                        null,
                        null),
                arguments(
                        Map.of(
                                HttpsConnector.enabled,
                                true,
                                HttpsConnector.listen_address,
                                new SocketAddress("localhost", 0)),
                        null,
                        LifecycleException.class,
                        "HTTPS set to enabled, but no SSL policy provided"));
    }

    @Nested
    class ConfigurationIT {
        @ParameterizedTest
        @MethodSource("org.neo4j.graphdb.facade.DatabaseManagementServiceFactoryIT#serverSettings")
        void shouldEnableWebServerIfConfiguredAndNecessary(
                Map<Setting<?>, Object> settings,
                Class<?> expectedServerClass,
                Class<? extends Exception> expectedException,
                String execptedMessage) {

            assertTrue(Files.isDirectory(testDirectory.directory("certificates", "certificates", "https")));
            Config cfg = Config.newBuilder()
                    .set(neo4j_home, testDirectory.homePath().toAbsolutePath())
                    .set(preallocate_logical_logs, false)
                    .set(settings)
                    .build();
            DatabaseManagementServiceFactory databaseManagementServiceFactory =
                    new DatabaseManagementServiceFactory(DbmsInfo.COMMUNITY, CommunityEditionModule::new);
            DatabaseManagementService dbms = null;
            try {
                if (expectedException != null) {
                    // When HTTPs is enabled, server startup fails due to missing SSL policy. This is fine for this
                    // test.
                    var cause = assertThrows(
                                    RuntimeException.class,
                                    () -> databaseManagementServiceFactory.build(
                                            cfg, false, GraphDatabaseDependencies.newDependencies()))
                            .getCause();
                    assertTrue(expectedException.isInstance(cause));
                    assertTrue(cause.getMessage().contains(execptedMessage));
                } else {
                    dbms = databaseManagementServiceFactory.build(
                            cfg, false, GraphDatabaseDependencies.newDependencies());
                    var dependencyResolver =
                            ((GraphDatabaseAPI) dbms.database(SYSTEM_DATABASE_NAME)).getDependencyResolver();
                    assertDoesNotThrow(() -> dependencyResolver.resolveDependency(expectedServerClass));
                }
            } finally {
                if (dbms != null) {
                    dbms.shutdown();
                }
            }
        }
    }

    private static class ShutdownListenerDatabaseEventListener extends DatabaseEventListenerAdapter {
        private final AtomicLong shutdownInvocations = new AtomicLong();

        @Override
        public void databaseShutdown(DatabaseEventContext eventContext) {
            shutdownInvocations.incrementAndGet();
        }

        long getShutdownInvocations() {
            return shutdownInvocations.get();
        }
    }
}
