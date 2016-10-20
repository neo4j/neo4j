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
package org.neo4j.causalclustering;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.discovery.SharedDiscoveryService;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.discovery.Cluster.buildAddresses;
import static org.neo4j.helpers.collection.Iterators.set;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.pagecache.PageSwapperFactoryForTesting.TEST_PAGESWAPPER_NAME;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;

/**
 * At the time of writing this test, certain operations required by Causal Clustering
 * do not work with custom IO configurations. This test ensures
 * that we fail gracefully with a helpful error message if the user
 * tries to combine Causal Clustering with custom IO configurations.
 * Specifically, the functionality that Causal Clustering needs that
 * custom IO configurations does not support is store copying.
 */

public class RejectCustomIOTest
{
    private DiscoveryServiceFactory discovery = new SharedDiscoveryService();

    @Rule
    public TestDirectory storeDir = TestDirectory.testDirectory();

    @Test
    public void shouldFailToStartWithCustomIOConfigurationInCoreModeTest() throws Exception
    {
        try
        {
            Map<String,String> extraParams =
                    stringMap( GraphDatabaseSettings.pagecache_swapper.name(), TEST_PAGESWAPPER_NAME );
            CoreClusterMember clusterMember = new CoreClusterMember( 0, 3, buildAddresses( set( 0, 1, 2 ) ), discovery,
                    defaultFormat().toString(), storeDir.directory(), extraParams, emptyMap() );
            clusterMember.start();
            fail( "Should not have created database with custom IO configuration in Core Mode." );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( CoreGraphDatabase.CUSTOM_IO_EXCEPTION_MESSAGE, ex.getMessage() );
        }
    }

    @Test
    public void shouldFailToStartWithCustomIOConfigurationInReadReplicaModeTest() throws Exception
    {
        try
        {
            DiscoveryServiceFactory discovery = new SharedDiscoveryService();
            Map<String,String> extraParams =
                    stringMap( GraphDatabaseSettings.pagecache_swapper.name(), TEST_PAGESWAPPER_NAME );
            ReadReplica clusterMember =
                    new ReadReplica( storeDir.directory(), 2, discovery, buildAddresses( set( 0, 1, 2 ) ),
                            extraParams, emptyMap(), defaultFormat().toString() );
            clusterMember.start();
            fail( "Should not have created database with custom IO configuration in Read Replica Mode." );
        }
        catch ( RuntimeException ex )
        {
            assertEquals( ReadReplicaGraphDatabase.CUSTOM_IO_EXCEPTION_MESSAGE, ex.getMessage() );
        }
    }
}
