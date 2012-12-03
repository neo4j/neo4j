/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.ha;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.ha.ClusterManager.clusterWithAdditionalClients;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesMembers;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

public class TestClusterClientPadding
{
    private static TargetDirectory dir = TargetDirectory.forTest( TestClusterClientPadding.class );
    private LifeSupport life = new LifeSupport();
    private ClusterManager clusterManager;
    private ManagedCluster cluster;
    
    @Before
    public void before() throws Throwable
    {
        clusterManager = life.add( new ClusterManager( clusterWithAdditionalClients( 2, 1 ), dir.directory( "dbs", true ), stringMap() ) );
        
        life.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( masterSeesMembers( 3 ) );
    }

    @After
    public void after() throws Throwable
    {
        life.stop();
    }
    
    @Test
    public void additionalClusterClientCanHelpBreakTiesWhenMasterIsShutDown() throws Throwable
    {
        HighlyAvailableGraphDatabase sittingMaster = cluster.getMaster();
        cluster.shutdown( sittingMaster );
        cluster.await( masterAvailable( sittingMaster ) );
    }

    @Test
    public void additionalClusterClientCanHelpBreakTiesWhenMasterFails() throws Throwable
    {
        HighlyAvailableGraphDatabase sittingMaster = cluster.getMaster();
        cluster.fail( sittingMaster );
        cluster.await( masterAvailable( sittingMaster ) );
    }
}
