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
package org.neo4j.dbms.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

@TestDirectoryExtension
@DbmsExtension()
class DbmsRuntimeSystemGraphComponentTest {

    // The trick here is that we pass a normal user database
    // to the tested system graph component, which will treat it as a system database
    @Inject
    private GraphDatabaseService userDatabase;

    private GraphDatabaseService fakeSystemDb;

    private SystemGraphComponents systemGraphComponents;
    private DbmsRuntimeSystemGraphComponent dbmsRuntimeSystemGraphComponent;

    @BeforeEach
    void beforeEach() {
        var systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder();
        fakeSystemDb = new FakeSystemDb(userDatabase);
        initDbmsComponent(systemGraphComponentsBuilder, false);
        systemGraphComponents = systemGraphComponentsBuilder.build();
    }

    @Test
    void testInitialisationOnFreshDatabase() {
        systemGraphComponents.initializeSystemGraph(fakeSystemDb);
        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);

        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    @Test
    void testInitialisationOnExistingDatabase() {
        createVersionNode(userDatabase);

        systemGraphComponents.initializeSystemGraph(fakeSystemDb);

        assertVersion(DbmsRuntimeVersion.V5_0, fakeSystemDb);

        assertStatus(SystemGraphComponent.Status.UNINITIALIZED);
    }

    @Test
    void testInitialisationOnExistingDatabaseWithAutomaticUpgrade() {
        var systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder();
        initDbmsComponent(systemGraphComponentsBuilder, true);
        var systemGraphComponents = systemGraphComponentsBuilder.build();

        createVersionNode(userDatabase);

        systemGraphComponents.initializeSystemGraph(fakeSystemDb);
        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);

        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    @Test
    void testCurrentVersionPresent() {
        createVersionNode(userDatabase, LatestVersions.LATEST_RUNTIME_VERSION);

        systemGraphComponents.initializeSystemGraph(fakeSystemDb);
        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);

        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    @Test
    void testOldVersionPresent() {
        createVersionNode(userDatabase, DbmsRuntimeVersion.V4_2);

        systemGraphComponents.initializeSystemGraph(fakeSystemDb);
        assertVersion(DbmsRuntimeVersion.V4_2, fakeSystemDb);

        assertStatus(SystemGraphComponent.Status.REQUIRES_UPGRADE);
    }

    @Test
    void testUpgrade() throws Exception {
        createVersionNode(userDatabase, DbmsRuntimeVersion.V4_2);

        assertStatus(SystemGraphComponent.Status.REQUIRES_UPGRADE);

        systemGraphComponents.upgradeToCurrent(fakeSystemDb);

        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);
        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    @Test
    void upgradeFromUninitialized() throws Exception {
        assertStatus(SystemGraphComponent.Status.UNINITIALIZED);

        systemGraphComponents.upgradeToCurrent(fakeSystemDb);

        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);
        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    @Test
    void initializeRuntimeVersionNodeToLatestVersion() {
        createRuntimeComponentNode(userDatabase, LatestVersions.LATEST_RUNTIME_VERSION);
        assertStatus(SystemGraphComponent.Status.CURRENT);

        systemGraphComponents.initializeSystemGraph(fakeSystemDb);

        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);
        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    @Test
    void updateRuntimeVersionNodeToLatestVersion() throws Exception {
        createRuntimeComponentNode(userDatabase, DbmsRuntimeVersion.V4_2);

        systemGraphComponents.upgradeToCurrent(fakeSystemDb);

        assertVersion(LatestVersions.LATEST_RUNTIME_VERSION, fakeSystemDb);
        assertStatus(SystemGraphComponent.Status.CURRENT);
    }

    private void initDbmsComponent(
            SystemGraphComponents.Builder systemGraphComponentsBuilder, boolean automaticUpgrade) {
        var config = Config.newBuilder()
                .set(GraphDatabaseInternalSettings.automatic_upgrade_enabled, automaticUpgrade)
                .build();
        dbmsRuntimeSystemGraphComponent = new DbmsRuntimeSystemGraphComponent(config);
        systemGraphComponentsBuilder.register(dbmsRuntimeSystemGraphComponent);
    }

    private void assertVersion(ComponentVersion expectedVersion, GraphDatabaseService system) {
        systemGraphComponents.forEach(component -> {
            if (component instanceof DbmsRuntimeSystemGraphComponent) {
                DbmsRuntimeVersion foundVersion =
                        ((DbmsRuntimeSystemGraphComponent) component).fetchStateFromSystemDatabase(system);
                assertEquals(expectedVersion, foundVersion);
            }
        });
    }

    private void assertStatus(SystemGraphComponent.Status expectedStatus) {
        try (var tx = userDatabase.beginTx()) {
            assertEquals(expectedStatus, dbmsRuntimeSystemGraphComponent.detect(tx));
        }
    }

    private static void createVersionNode(GraphDatabaseService database) {
        try (var tx = database.beginTx()) {
            tx.createNode(VERSION_LABEL);
            tx.commit();
        }
    }

    private static void createVersionNode(GraphDatabaseService database, DbmsRuntimeVersion version) {
        try (var tx = database.beginTx()) {
            tx.createNode(VERSION_LABEL).setProperty(DBMS_RUNTIME_COMPONENT.name(), version.getVersion());
            tx.commit();
        }
    }

    private static void createRuntimeComponentNode(GraphDatabaseService database, DbmsRuntimeVersion version) {
        try (var tx = database.beginTx()) {
            tx.createNode(DbmsRuntimeSystemGraphComponent.OLD_COMPONENT_LABEL)
                    .setProperty(DbmsRuntimeSystemGraphComponent.OLD_PROPERTY_NAME, version.getVersion());
            tx.commit();
        }
    }

    private static class FakeSystemDb implements GraphDatabaseService {

        private final GraphDatabaseService wrappedDb;

        FakeSystemDb(GraphDatabaseService wrappedDb) {
            this.wrappedDb = wrappedDb;
        }

        @Override
        public boolean isAvailable() {
            return wrappedDb.isAvailable();
        }

        @Override
        public boolean isAvailable(long timeoutMillis) {
            return wrappedDb.isAvailable(timeoutMillis);
        }

        @Override
        public Transaction beginTx() {
            return wrappedDb.beginTx();
        }

        @Override
        public Transaction beginTx(long timeout, TimeUnit unit) {
            return wrappedDb.beginTx(timeout, unit);
        }

        @Override
        public void executeTransactionally(String query) throws QueryExecutionException {
            throw new IllegalStateException("Not implemented");
        }

        @Override
        public void executeTransactionally(String query, Map<String, Object> parameters)
                throws QueryExecutionException {
            throw new IllegalStateException("Not implemented");
        }

        @Override
        public <T> T executeTransactionally(
                String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer)
                throws QueryExecutionException {
            throw new IllegalStateException("Not implemented");
        }

        @Override
        public <T> T executeTransactionally(
                String query, Map<String, Object> parameters, ResultTransformer<T> resultTransformer, Duration timeout)
                throws QueryExecutionException {
            throw new IllegalStateException("Not implemented");
        }

        @Override
        public String databaseName() {
            return "system";
        }
    }
}
