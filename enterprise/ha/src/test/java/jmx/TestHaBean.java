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
package jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.ha.CreateEmptyDb;
import org.neo4j.helpers.Pair;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.ClusterMemberState;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.TargetDirectory;

public class TestHaBean
{
    private static final TargetDirectory dir = TargetDirectory.forTest( TestHaBean.class );
    private static HighlyAvailableGraphDatabase database;

    @BeforeClass
    public static void startDb() throws Exception
    {
        File storeDir = dir.graphDbDir( /*clean=*/true );
        CreateEmptyDb.at( storeDir );
        database = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( storeDir.getAbsolutePath() )
                .setConfig( HaSettings.server_id, "1" )
                .setConfig( "jmx.port", "9913" )
                .setConfig( HaSettings.ha_server, ":1137" )
                .newGraphDatabase();
    }

    @AfterClass
    public static void stopDb() throws Exception
    {
        if ( database != null )
        {
            database.shutdown();
        }
        database = null;
        dir.cleanup();
    }

    @Test
    public void canGetHaBean() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        assertNotNull( "could not get ha bean", ha );
        assertEquals( "single instance should be master", ClusterMemberState.MASTER.name(), ha.getInstanceState() );
        ClusterMemberInfo info = ha.getInstancesInCluster()[0];
        assertEquals("single instance should be the returned instance id", info.getInstanceId(), "1");
        assertTrue( "single instance should have coordinator role", Arrays.equals( info.getRoles(),
                new String[]{ClusterConfiguration.COORDINATOR} ) );
        assertEquals( "single instance should be coordinator", ClusterConfiguration.COORDINATOR, info.getStatus() );
    }

    @Test
    public void canGetBranchedStoreBean() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
        BranchedStore bs = neo4j.getBranchedStoreBean();
        assertNotNull( "could not get ha bean", bs );
        assertEquals( "no branched stores for new db", 0,
                bs.getBranchedStores().length );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canGetInstanceConnectionInformation() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
        ClusterMemberInfo[] clusterMembers = neo4j.getHighAvailabilityBean().getInstancesInCluster();
        assertNotNull( clusterMembers );
        assertEquals( 1, clusterMembers.length );
        ClusterMemberInfo clusterMember = clusterMembers[0];
        assertNotNull( clusterMember );
//        String address = clusterMember.getAddress();
//        assertNotNull( "No JMX address for instance", address );
        String id = clusterMember.getInstanceId();
        assertNotNull( "No instance id", id );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canConnectToInstance() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        ClusterMemberInfo[] clusterMembers = ha.getInstancesInCluster();
        assertNotNull( clusterMembers );
        assertEquals( 1, clusterMembers.length );
        ClusterMemberInfo clusterMember = clusterMembers[0];
        assertNotNull( clusterMember );
        Pair<Neo4jManager, HighAvailability> proc = clusterMember.connect();
        assertNotNull( "could not connect", proc );
        neo4j = proc.first();
        ha = proc.other();
        assertNotNull( neo4j );
        assertNotNull( ha );

        clusterMembers = ha.getInstancesInCluster();
        assertNotNull( clusterMembers );
        assertEquals( 1, clusterMembers.length );
//        assertEquals( clusterMember.getAddress(), clusterMembers[0].getAddress() );
        assertEquals( clusterMember.getInstanceId(), clusterMembers[0].getInstanceId() );
    }
}
