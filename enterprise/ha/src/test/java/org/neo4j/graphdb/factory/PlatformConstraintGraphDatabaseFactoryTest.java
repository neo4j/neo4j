/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.factory;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.ha.factory.HighlyAvailableEditionModule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.pagecache.PageSwapperFactoryForTesting.TEST_PAGESWAPPER_NAME;

public class PlatformConstraintGraphDatabaseFactoryTest
{
    @Rule
    public TestDirectory storeDir = TestDirectory.testDirectory();

    private File workingDir;

    @Before
    public void setup()
    {
        workingDir = storeDir.directory( "working" );
    }

    @Test
    public void shouldFailToStartWithCustomIOConfigurationTest()
    {
        try
        {
            createGraphDatabaseService();
            fail( "Should not have created database with custom IO configuration and HA mode." );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( HighlyAvailableEditionModule.CUSTOM_IO_EXCEPTION_MESSAGE, ex.getMessage() );
        }
    }

    private GraphDatabaseService createGraphDatabaseService()
    {
        return new TestHighlyAvailableGraphDatabaseFactory().newEmbeddedDatabaseBuilder( workingDir )
                .setConfig( GraphDatabaseSettings.pagecache_swapper, TEST_PAGESWAPPER_NAME )
                .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                .setConfig( ClusterSettings.server_id, "1" ).newGraphDatabase();
    }

}
