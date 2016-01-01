/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.junit.rules.TemporaryFolder;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class DiagnosticsLoggingTest
{
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private final FakeLogger logger = new FakeLogger();

    @Test
    public void shouldSeeExpectedDiagnostics()
    {
        // GIVEN
        GraphDatabaseService db = build().
                setConfig( GraphDatabaseSettings.dump_configuration, Settings.TRUE ).
                setConfig( GraphDatabaseSettings.pagecache_memory, "4M" ).
                newGraphDatabase();

        // WHEN
        String messages = logger.getMessages();

        // THEN we should have logged
        // environment diagnostics
        assertThat( messages, containsString( "Network information" ) );
        assertThat( messages, containsString( "Disk space on partition" ) );
        assertThat( messages, containsString( "Local timezone" ) );
        // page cache info
        assertThat( messages, containsString( "Page cache size: 4 MiB" ) );
        // neostore records
        for ( NeoStore.Position position : NeoStore.Position.values() )
        {
            assertThat( messages, containsString( position.name() ) );
        }
        // transaction log info
        assertThat( messages, containsString( "Transaction log" ) );
        db.shutdown();
    }

    private GraphDatabaseBuilder build()
    {
        String storeDir = folder.getRoot().getAbsolutePath();
        return new TestGraphDatabaseFactory( logger ).newEmbeddedDatabaseBuilder( storeDir );
    }
}
