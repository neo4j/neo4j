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
package org.neo4j.commandline.dbms;

import static java.lang.Boolean.FALSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.Neo4jDatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.SystemGraphComponent.Status;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.facade.SystemDbUpgrader;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.SystemDatabaseMigrator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

@Neo4jLayoutExtension
public abstract class SystemDbUpgraderAbstractTestBase {
    @Inject
    protected Neo4jLayout databaseLayout;

    @Test
    void shouldOnlyStartSystemDb() throws Exception {
        createDatabase();

        var editionFactory = migrationEditionModuleFactory();
        var systemDatabaseMigrator = systemDatabaseMigrator();
        var eventListener = new StartedDatabaseEventListener();
        SystemDbUpgrader.upgrade(
                editionFactory,
                systemDatabaseMigrator,
                getConfig(databaseLayout.homeDirectory()),
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                eventListener);
        assertThat(eventListener.startedDatabases).containsExactly(SYSTEM_DATABASE_NAME);
    }

    @Test
    void shouldMigrateSystemDatabase() throws Exception {
        var homeDirectory = databaseLayout.homeDirectory();
        Unzip.unzip(getClass(), previousMajorsSystemDatabase(), homeDirectory);

        var editionFactory = migrationEditionModuleFactory();
        var systemDatabaseMigrator = systemDatabaseMigrator();
        SystemDbUpgrader.upgrade(
                editionFactory,
                systemDatabaseMigrator,
                getConfig(homeDirectory),
                NullLogProvider.getInstance(),
                NullLogProvider.getInstance(),
                new DatabaseEventListenerAdapter());

        var dbms = dbmsBuilder(homeDirectory)
                .setConfig(BoltConnector.enabled, FALSE)
                .setConfig(GraphDatabaseInternalSettings.automatic_upgrade_enabled, FALSE)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .setConfig(baseConfig())
                .build();

        var systemDatabase = dbms.database(SYSTEM_DATABASE_NAME);
        var systemGraphComponents = ((GraphDatabaseAPI) systemDatabase)
                .getDependencyResolver()
                .resolveDependency(SystemGraphComponents.class);

        assertThat(systemGraphComponents.detect(systemDatabase)).isEqualTo(Status.CURRENT);

        dbms.shutdown();
    }

    protected Config getConfig(Path homeDirectory) {
        return Config.newBuilder()
                .set(GraphDatabaseSettings.neo4j_home, homeDirectory)
                .build();
    }

    private void createDatabase() {
        DatabaseManagementService dbms = new DatabaseManagementServiceBuilder(databaseLayout.homeDirectory())
                .setConfig(BoltConnector.enabled, FALSE)
                .setConfig(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .setConfig(baseConfig())
                .build();
        dbms.shutdown();
    }

    protected abstract Map<Setting<?>, Object> baseConfig();

    protected abstract MigrationEditionModuleFactory migrationEditionModuleFactory();

    protected abstract SystemDatabaseMigrator systemDatabaseMigrator();

    protected abstract String previousMajorsSystemDatabase();

    protected abstract Neo4jDatabaseManagementServiceBuilder dbmsBuilder(Path homePath);

    private static class StartedDatabaseEventListener extends DatabaseEventListenerAdapter {
        private final List<String> startedDatabases = new ArrayList<>();

        @Override
        public void databaseStart(DatabaseEventContext eventContext) {
            startedDatabases.add(eventContext.getDatabaseName());
        }
    }
}
