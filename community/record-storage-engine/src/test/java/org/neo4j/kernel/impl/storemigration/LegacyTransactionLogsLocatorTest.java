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
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( TestDirectoryExtension.class )
class LegacyTransactionLogsLocatorTest
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void transactionLogsDirectoryEqualsToDatabaseDirectoryWithDefaultConfiguration()
    {
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( Config.defaults(), testDirectory.directory() );
        assertEquals( testDirectory.directory(), logsLocator.getTransactionLogsDirectory() );
    }

    @Test
    void transactionLogsDirectoryEqualsToLogicalLogLegacySettingsWhenConfigured()
    {
        File customDirectory = testDirectory.directory( "customDirectory" );
        Config config = Config.defaults( GraphDatabaseSettings.logical_logs_location, customDirectory.getAbsolutePath() );
        LegacyTransactionLogsLocator logsLocator = new LegacyTransactionLogsLocator( config, testDirectory.directory() );
        assertEquals( customDirectory, logsLocator.getTransactionLogsDirectory() );
    }
}
