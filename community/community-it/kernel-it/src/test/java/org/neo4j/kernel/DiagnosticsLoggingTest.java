/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.logging.LogAssertions.assertThat;

class DiagnosticsLoggingTest
{

    @Test
    void shouldSeeExpectedDiagnostics()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder()
                .setInternalLogProvider( logProvider )
                .impermanent()
                .setConfig( GraphDatabaseInternalSettings.dump_configuration, true )
                .setConfig( GraphDatabaseSettings.pagecache_memory, "4M" )
                .build();
        try
        {
            // THEN we should have logged
            assertThat( logProvider ).containsMessages( "Network information", "Local timezone", "Page cache: 4M" );
            // neostore records
            for ( MetaDataStore.Position position : MetaDataStore.Position.values() )
            {
                assertThat( logProvider ).containsMessages( position.name() );
            }
            // transaction log info
            assertThat( logProvider ).containsMessages( "Transaction log", "TimeZone version: " );
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
