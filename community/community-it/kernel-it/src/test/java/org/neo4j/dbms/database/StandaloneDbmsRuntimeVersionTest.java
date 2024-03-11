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

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.LatestVersions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;

@TestDirectoryExtension
@DbmsExtension()
class StandaloneDbmsRuntimeVersionTest {
    private static final DbmsRuntimeVersion OVERRIDDEN_INITIAL_LATEST_VERSION = DbmsRuntimeVersion.V5_0;
    private static final DbmsRuntimeVersion OLDER_VERSION = DbmsRuntimeVersion.V4_4;

    @Inject
    private DatabaseContextProvider<DatabaseContext> databaseContextProvider;

    @Inject
    private StandaloneDbmsRuntimeVersionProvider dbmsRuntimeVersionProvider;

    private GraphDatabaseService systemDb;

    @BeforeEach
    void beforeEach() {
        systemDb = databaseContextProvider
                .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                .get()
                .databaseFacade();
    }

    @Test
    void testBasicVersionLifecycle() {
        // the system DB will be initialised with the default version for this binary
        assertSame(LatestVersions.LATEST_RUNTIME_VERSION, dbmsRuntimeVersionProvider.getVersion());

        // BTW this should never be manipulated directly outside tests
        setRuntimeVersion(OLDER_VERSION);
        assertSame(OLDER_VERSION, dbmsRuntimeVersionProvider.getVersion());

        systemDb.executeTransactionally("CALL dbms.upgrade()");

        assertSame(LatestVersions.LATEST_RUNTIME_VERSION, dbmsRuntimeVersionProvider.getVersion());
    }

    @Test
    void latestVersionIsRealLatestVersionByDefault() {
        // The system DB will be initialised with the default version for this binary
        assertSame(LatestVersions.LATEST_RUNTIME_VERSION, dbmsRuntimeVersionProvider.getVersion());
    }

    @Test
    @DbmsExtension(configurationCallback = "configuration")
    void latestVersionCanBeSetThroughConfigForTests() {
        // The system DB should be initialised with what we think is latest
        assertSame(OVERRIDDEN_INITIAL_LATEST_VERSION, dbmsRuntimeVersionProvider.getVersion());

        // And will not get upgraded past that "latest" version
        // BTW this should never be manipulated directly outside tests
        setRuntimeVersion(OLDER_VERSION);
        assertSame(OLDER_VERSION, dbmsRuntimeVersionProvider.getVersion());

        systemDb.executeTransactionally("CALL dbms.upgrade()");

        assertSame(OVERRIDDEN_INITIAL_LATEST_VERSION, dbmsRuntimeVersionProvider.getVersion());
    }

    @ExtensionCallback
    void configuration(TestDatabaseManagementServiceBuilder builder) {
        builder.setConfig(
                GraphDatabaseInternalSettings.latest_runtime_version, OVERRIDDEN_INITIAL_LATEST_VERSION.getVersion());
    }

    private void setRuntimeVersion(DbmsRuntimeVersion runtimeVersion) {
        try (var tx = systemDb.beginTx();
                ResourceIterator<Node> nodes = tx.findNodes(VERSION_LABEL)) {
            nodes.stream()
                    .forEach(dbmsRuntimeNode -> dbmsRuntimeNode.setProperty(
                            ComponentVersion.DBMS_RUNTIME_COMPONENT.name(), runtimeVersion.getVersion()));

            tx.commit();
        }

        dbmsRuntimeVersionProvider.setVersion(runtimeVersion);
    }
}
