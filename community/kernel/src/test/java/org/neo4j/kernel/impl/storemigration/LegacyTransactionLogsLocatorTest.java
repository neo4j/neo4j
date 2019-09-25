/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Neo4jLayoutExtension
class LegacyTransactionLogsLocatorTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;
    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void transactionLogsDirectoryEqualsToDatabaseDirectoryWithDefaultConfiguration()
    {
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( Config.defaults(), databaseLayout );
        assertEquals( databaseLayout.databaseDirectory(), logsLocator.getTransactionLogsDirectory() );
    }

    @Test
    void transactionLogsDirectoryEqualsToLogicalLogLegacySettingsWhenConfigured()
    {
        File customDirectory = testDirectory.directory( "customDirectory" );
        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location, customDirectory.toPath().toAbsolutePath() );
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( config, databaseLayout );
        assertEquals( customDirectory, logsLocator.getTransactionLogsDirectory() );
    }

    @Test
    void transactionLogsDirectoryEqualsToDatabaseDirectoryForSystemDatabase()
    {
        File customDirectory = testDirectory.directory( "customDirectory" );
        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location, customDirectory.toPath().toAbsolutePath() );
        DatabaseLayout systemDbLayout = neo4jLayout.databaseLayout( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( config, systemDbLayout );
        assertEquals( systemDbLayout.databaseDirectory(), logsLocator.getTransactionLogsDirectory() );
    }
}
