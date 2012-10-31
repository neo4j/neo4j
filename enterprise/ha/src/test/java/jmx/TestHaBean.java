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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.net.InetAddress;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.TargetDirectory;

public class TestHaBean
{
    private static final TargetDirectory dir = TargetDirectory.forTest( TestHaBean.class );
    private static HighlyAvailableGraphDatabase database;
    private static HighlyAvailableGraphDatabase secondDatabase;

    @BeforeClass
    public static void startDb() throws Exception
    {
        File storeDir = dir.directory( "1", /*clean=*/true );
        database = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( storeDir.getAbsolutePath() )
                .setConfig( HaSettings.server_id, "1" )
                .setConfig( "jmx.port", "9913" )
                .setConfig( HaSettings.ha_server, ":1137" )
                .newGraphDatabase();
        database.beginTx().finish();
    }

    private void startSecondDatabase()
    {
        if ( secondDatabase != null )
            return;
        
        File storeDir = dir.directory( "2", /*clean=*/true );
        secondDatabase = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( storeDir.getAbsolutePath() )
                .setConfig( HaSettings.server_id, "2" )
                .setConfig( "jmx.port", "9914" )
                .setConfig( HaSettings.ha_server, ":1138" )
                .setConfig( HaSettings.initial_hosts, ":5001" )
                .newGraphDatabase();
        secondDatabase.beginTx().finish();
    }
    
    @AfterClass
    public static void stopDb() throws Exception
    {
        if ( database != null )
        {
            database.shutdown();
        }
        database = null;
        stopSecondDb();
        dir.cleanup();
    }
    
    private static void stopSecondDb() throws Exception
    {
        if ( secondDatabase != null )
        {
            secondDatabase.shutdown();
            secondDatabase = null;
        }
    }
    
//    private static void failSecondDb() throws Exception
//    {
//        ClusterClient clusterClient = secondDatabase.getDependencyResolver().resolveDependency( ClusterClient.class );
//        LifeSupport clusterClientLife = (LifeSupport) accessible( clusterClient.getClass().getDeclaredField( "life" ) ).get( clusterClient );
//        clusterClientLife.remove( instance( NetworkInstance.class, clusterClientLife.getLifecycleInstances() ) );
//    }
//
//    private static <T> T instance( Class<T> classToFind, Iterable<?> from )
//    {
//        for ( Object item : from )
//            if ( classToFind.isAssignableFrom( item.getClass() ) )
//                return (T) item;
//        fail( "Couldn't find the network instance to fail. Internal field, so fragile sensitive to changes though" );
//        return null; // it will never get here.
//    }
//
//    private static Field accessible( Field field )
//    {
//        field.setAccessible( true );
//        return field;
//    }

    @Test
    public void canGetHaBean() throws Exception
    {
        Neo4jManager neo4j = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        assertNotNull( "could not get ha bean", ha );
        assertMasterInformation( ha );
    }

    private void assertMasterInformation( HighAvailability ha )
    {
        assertTrue( "single instance should be master and available", ha.isAvailable() );
        assertEquals( "single instance should be master", HighAvailabilityMemberState.MASTER.name(), ha.getRole() );
        ClusterMemberInfo info = ha.getInstancesInCluster()[0];
        assertEquals( "single instance should be the returned instance id", "1", info.getInstanceId() );
        assertTrue( "single instance should have coordinator cluster role", Arrays.equals( info.getClusterRoles(),
                new String[]{ClusterConfiguration.COORDINATOR} ) );
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
    
    @Test
    public void joinedInstanceShowsUpAsSlave() throws Exception
    {
        startSecondDatabase();
        HighAvailability ha = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) ).getHighAvailabilityBean();
        ClusterMemberInfo[] instancesInCluster = ha.getInstancesInCluster();
        assertEquals( 2, instancesInCluster.length );
        HighAvailability secondHa = new Neo4jManager( secondDatabase.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) ).getHighAvailabilityBean();
        ClusterMemberInfo[] secondInstancesInCluster = secondHa.getInstancesInCluster();
        assertEquals( 2, secondInstancesInCluster.length );
        
        assertMasterAndSlaveInformation( instancesInCluster );
        assertMasterAndSlaveInformation( secondInstancesInCluster );
    }
    
    @Test
    public void leftInstanceDisappearsFromMemberList() throws Exception
    {
        // Start the second db and make sure it's visible in the member list.
        // Then shut it down to see if it disappears from the member list again.
        startSecondDatabase();
        HighAvailability secondHa = new Neo4jManager( secondDatabase.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) ).getHighAvailabilityBean();
        assertEquals( 2, secondHa.getInstancesInCluster().length );
        stopSecondDb();
        
        Neo4jManager neo4j = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
        HighAvailability ha = neo4j.getHighAvailabilityBean();
        assertEquals( 1, ha.getInstancesInCluster().length );
        assertMasterInformation( ha );
    }
    
    @Ignore
    @Test
    public void failedMemberIsStillInMemberListAlthoughUnavailable() throws Exception
    {
        startSecondDatabase();
        HighAvailability secondHa = new Neo4jManager( secondDatabase.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) ).getHighAvailabilityBean();
        assertEquals( 2, secondHa.getInstancesInCluster().length );
        // failSecondDb();
        
        HighAvailability ha = new Neo4jManager( database.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) ).getHighAvailabilityBean();
        long end = System.currentTimeMillis() + SECONDS.toMillis( 300 );
        boolean available = false;
        while ( System.currentTimeMillis() < end )
        {
            available = member( ha.getInstancesInCluster(), 5002 ).isAvailable();
            if ( !available )
                break;
            Thread.sleep( 500 );
        }
        if ( available )
            fail( "Failed instance didn't show up as such in JMX" );
    }

    private void assertMasterAndSlaveInformation( ClusterMemberInfo[] instancesInCluster ) throws Exception
    {
        ClusterMemberInfo master = member( instancesInCluster, 5001 );
        assertEquals( "1", master.getInstanceId() );
        assertEquals( HighAvailabilityMemberState.MASTER.name(), master.getHaRole() );
        assertTrue( "Unexpected start of HA URI" + uri( "ha", master.getUris() ),
                uri( "ha", master.getUris() ).startsWith( "ha://" + InetAddress.getLocalHost().getHostAddress() + ":1137" ) );
        assertTrue( "Master not available", master.isAvailable() );

        ClusterMemberInfo slave = member( instancesInCluster, 5002 );
        assertEquals( "2", slave.getInstanceId() );
        assertEquals( HighAvailabilityMemberState.SLAVE.name(), slave.getHaRole() );
        assertTrue( "Unexpected start of HA URI" + uri( "ha", slave.getUris() ),
                uri( "ha", slave.getUris() ).startsWith( "ha://" + InetAddress.getLocalHost().getHostAddress() + ":1138" ) );
        assertTrue( "Slave not available", slave.isAvailable() );
    }

    private String uri( String scheme, String[] uris )
    {
        for ( String uri : uris )
            if ( uri.startsWith( scheme ) )
                return uri;
        fail( "Couldn't find '" + scheme + "' URI among " + Arrays.toString( uris ) );
        return null; // it will never get here.
    }

    private ClusterMemberInfo member( ClusterMemberInfo[] members, int clusterPort )
    {
        for ( ClusterMemberInfo member : members )
            if ( uri( "cluster", member.getUris() ).endsWith( ":" + clusterPort ) )
                return member;
        fail( "Couldn't find cluster member with cluster URI port " + clusterPort + " among " + Arrays.toString( members ) );
        return null; // it will never get here.
    }
}
