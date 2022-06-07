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
package org.neo4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.internal.schema.IndexType.RANGE;
import static org.neo4j.storemigration.StoreMigrationTestUtils.runStoreMigrationCommandFromSameJvm;

import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.storemigration.InitialIndexStateMonitor;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public class SystemDbCommunityMigrationIT {
    @Inject
    Neo4jLayout databaseLayout;

    @Test
    void shouldUpgradeSystemDbComponentsAndPopulateIndexes() throws Exception {
        // todo:
        //  1. Make community db to test properly
        var homeDirectory = databaseLayout.homeDirectory();
        Unzip.unzip(SystemDbCommunityMigrationIT.class, "AF4.3.0_V4.4_empty.zip", homeDirectory);

        runStoreMigrationCommandFromSameJvm(databaseLayout, "--database", SYSTEM_DATABASE_NAME);

        var initialIndexStateMonitor = new InitialIndexStateMonitor(SYSTEM_DATABASE_NAME);
        var dbms = createDbmsBuilder(homeDirectory)
                // Make sure system db is not automatically upgraded, because it will hide malfunction in migration
                // command
                .setConfig(GraphDatabaseSettings.allow_single_automatic_upgrade, false)
                .setMonitors(initialIndexStateMonitor.monitors())
                .build();
        try {
            var system = dbms.database(SYSTEM_DATABASE_NAME);

            assertThat(initialIndexStateMonitor.allIndexStates).isNotEmpty();
            for (Map.Entry<IndexDescriptor, InternalIndexState> internalIndexStateEntry :
                    initialIndexStateMonitor.allIndexStates.entrySet()) {
                assertThat(internalIndexStateEntry.getKey().getIndexType()).isIn(LOOKUP, RANGE);
                assertThat(internalIndexStateEntry.getValue())
                        .withFailMessage(internalIndexStateEntry.getKey() + " was not ONLINE as expected: "
                                + internalIndexStateEntry.getValue())
                        .isEqualTo(ONLINE);
            }
            var systemGraphComponents =
                    ((GraphDatabaseAPI) system).getDependencyResolver().resolveDependency(SystemGraphComponents.class);

            try (Transaction tx = system.beginTx()) {
                systemGraphComponents.forEach(component -> assertCurrent(tx, component));
                tx.commit();
            }
        } finally {
            dbms.shutdown();
        }
    }

    // To be overloaded by enterprise test
    protected TestDatabaseManagementServiceBuilder createDbmsBuilder(Path homeDirectory) {
        return new TestDatabaseManagementServiceBuilder(homeDirectory);
    }

    private static void assertCurrent(Transaction tx, SystemGraphComponent component) {
        var status = component.detect(tx);
        assertThat(status)
                .withFailMessage(
                        "SystemGraphComponent " + component.componentName() + " was not upgraded, state=" + status)
                .isEqualTo(SystemGraphComponent.Status.CURRENT);
    }
}
