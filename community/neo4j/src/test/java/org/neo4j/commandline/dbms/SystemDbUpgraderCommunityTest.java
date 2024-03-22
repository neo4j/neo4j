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

import java.nio.file.Path;
import java.util.Map;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.api.Neo4jDatabaseManagementServiceBuilder;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.module.edition.migration.CommunityMigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.CommunitySystemDatabaseMigrator;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.SystemDatabaseMigrator;

public class SystemDbUpgraderCommunityTest extends SystemDbUpgraderAbstractTestBase {
    @Override
    protected Map<Setting<?>, Object> baseConfig() {
        return Map.of();
    }

    @Override
    protected MigrationEditionModuleFactory migrationEditionModuleFactory() {
        return new CommunityMigrationEditionModuleFactory();
    }

    @Override
    protected SystemDatabaseMigrator systemDatabaseMigrator() {
        return new CommunitySystemDatabaseMigrator();
    }

    /**
     * This store is created by the following steps:
     * - Start a 4.4.0 Community DBMS
     * - Create an additional user
     * - Stop DBMS
     * - Rename the system store to something else in order that the StoreMigrate command does not treat is as system, so no upgrade will be performed right away
     * - Perform store migration with the StoreMigrate command, use the admin tool of 5.1.0
     * - Rename it back (enough to keep system, the default databases can be removed)
     */
    @Override
    protected String previousMajorsSystemDatabase() {
        return "44components5storeCommunitySystem.zip";
    }

    @Override
    protected Neo4jDatabaseManagementServiceBuilder dbmsBuilder(Path homePath) {
        return new DatabaseManagementServiceBuilder(homePath);
    }
}
