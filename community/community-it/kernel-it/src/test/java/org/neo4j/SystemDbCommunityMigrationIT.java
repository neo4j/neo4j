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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.MigrateStoreCommand;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.SystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.IndexMonitor;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.monitoring.Monitors;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import picocli.CommandLine;

@Neo4jLayoutExtension
public class SystemDbCommunityMigrationIT {
    @Inject
    Neo4jLayout databaseLayout;

    @Test
    void shouldUpgradeSystemDbComponentsAndPopulateIndexes() throws Exception {
        // todo:
        //  1. Make community db to test properly
        //  2. Move the migration test util thing to community so that we can reach it from here
        var homeDirectory = databaseLayout.homeDirectory();
        Unzip.unzip(SystemDbCommunityMigrationIT.class, "AF4.3.0_V4.4_empty.zip", homeDirectory);

        runStoreMigrationCommandFromSameJvm(databaseLayout, "--database", SYSTEM_DATABASE_NAME);

        var allIndexMonitor = new AllIndexMonitor();
        var dbms = createDbmsBuilder(homeDirectory)
                // Make sure system db is not automatically upgraded, because it will hide malfunction in migration
                // command
                .setConfig(GraphDatabaseSettings.allow_single_automatic_upgrade, false)
                .setMonitors(allIndexMonitor.monitors())
                .build();
        try {
            var system = dbms.database(SYSTEM_DATABASE_NAME);

            assertThat(allIndexMonitor.allIndexStates).isNotEmpty();
            for (Map.Entry<IndexDescriptor, InternalIndexState> internalIndexStateEntry :
                    allIndexMonitor.allIndexStates.entrySet()) {
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

    public static void runStoreMigrationCommandFromSameJvm(Neo4jLayout neo4jLayout, String... args) {
        var homeDir = neo4jLayout.homeDirectory().toAbsolutePath();
        var configDir = homeDir.resolve("conf");
        var out = new Output();
        var err = new Output();

        var ctx = new ExecutionContext(
                homeDir, configDir, out.printStream, err.printStream, new DefaultFileSystemAbstraction());

        var command = CommandLine.populateCommand(new MigrateStoreCommand(ctx), args);

        try {
            int exitCode = command.call();
            new Result(exitCode, out.toString(), err.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static class Output {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final PrintStream printStream = new PrintStream(buffer);

        @Override
        public String toString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

    private record Result(int exitCode, String out, String err) {}

    private static class AllIndexMonitor extends IndexMonitor.MonitorAdapter {
        HashMap<IndexDescriptor, InternalIndexState> allIndexStates = new HashMap<>();

        @Override
        public void initialState(String databaseName, IndexDescriptor descriptor, InternalIndexState state) {
            if (databaseName.equals(SYSTEM_DATABASE_NAME)) {
                allIndexStates.put(descriptor, state);
            }
        }

        public Monitors monitors() {
            var monitors = new Monitors();
            monitors.addMonitorListener(this);
            return monitors;
        }
    }
}
