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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.CleanupRule;

public class DiagnosticsLoggingTest
{
    @Rule
    public CleanupRule cleanupRule = new CleanupRule();

    @Test
    public void shouldSeeExpectedDiagnostics()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        GraphDatabaseService db =
                new TestGraphDatabaseFactory().setInternalLogProvider( logProvider )
                .newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.dump_configuration, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.pagecache_memory, "4M" )
                .newGraphDatabase();
        cleanupRule.add( db );

        // THEN we should have logged
        logProvider.assertContainsMessageContaining( "Network information" );
        logProvider.assertContainsMessageContaining( "Disk space on partition" );
        logProvider.assertContainsMessageContaining( "Local timezone" );
        // page cache info
        logProvider.assertContainsMessageContaining( "Page cache: 4M" );
        // neostore records
        for ( MetaDataStore.Position position : MetaDataStore.Position.values() )
        {
            logProvider.assertContainsMessageContaining( position.name() );
        }
        // transaction log info
        logProvider.assertContainsMessageContaining( "Transaction log" );
        logProvider.assertContainsMessageContaining( "TimeZone version: " );
    }
}
