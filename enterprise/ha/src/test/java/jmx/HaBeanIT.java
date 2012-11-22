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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterSeesAllSlavesAsAvailable;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.MapUtil;
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
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterManager.RepairKit;

public class HaBeanIT
{
    private static final TargetDirectory dir = TargetDirectory.forTest( HaBeanIT.class );
    private ManagedCluster cluster;
    private ClusterManager clusterManager;
    
    public void startCluster( int size ) throws Throwable
    {
        clusterManager = new ClusterManager( clusterOfSize( size ), dir.directory( "dbs", true ), MapUtil.stringMap() )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, int serverId )
            {
                builder.setConfig( "jmx.port", "" + (9912+serverId) );
                builder.setConfig( HaSettings.ha_server, ":" + (1136+serverId) );
            }
        };
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( masterSeesAllSlavesAsAvailable() );
    }
    
    @After
    public void stopCluster() throws Throwable
    {
        clusterManager.stop();
    }
    
    public Neo4jManager beans( HighlyAvailableGraphDatabase db )
    {
        return new Neo4jManager( db.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
    }

    public HighAvailability ha( HighlyAvailableGraphDatabase db )
    {
        return beans( db ).getHighAvailabilityBean();
    }

    @Test
    public void canGetHaBean() throws Throwable
    {
        startCluster( 1 );
        HighAvailability ha = ha( cluster.getMaster() );
        assertNotNull( "could not get ha bean", ha );
        assertMasterInformation( ha );
    }

    @Test
    public void testLatestTxInfoIsCorrect() throws Throwable
    {
        startCluster( 1 );
        HighlyAvailableGraphDatabase db = cluster.getMaster();
        HighAvailability masterHa = ha( db );
        long lastCommitted = masterHa.getLastCommittedTxId();
        Transaction tx = db.beginTx();
        db.createNode();
        tx.success();
        tx.finish();
        assertEquals( lastCommitted + 1, masterHa.getLastCommittedTxId() );
    }

    @Test
    public void testUpdatePullWorksAndUpdatesLastUpdateTime() throws Throwable
    {
        startCluster( 2 );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Transaction tx = master.beginTx();
        master.createNode();
        tx.success();
        tx.finish();
        HighAvailability slaveBean = ha( slave );
        DateFormat format = new SimpleDateFormat( "yyyy-MM-DD kk:mm:ss.SSSZZZZ" );
        // To begin with, no updates
        assertEquals( "N/A", slaveBean.getLastUpdateTime() );
        slaveBean.update();
        long timeUpdated = format.parse( slaveBean.getLastUpdateTime() ).getTime();
        assertTrue( timeUpdated > 0 );
    }

    @Test
    public void testAfterGentleMasterSwitchClusterInfoIsCorrect() throws Throwable
    {
        startCluster( 3 );
        RepairKit masterShutdown = cluster.shutdown( cluster.getMaster() );
        cluster.await( ClusterManager.masterAvailable() );
        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            assertEquals( 2, ha( db ).getInstancesInCluster().length );
        }
        masterShutdown.repair();
        cluster.await( ClusterManager.masterAvailable() );
        cluster.await( ClusterManager.masterSeesAllSlavesAsAvailable() );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            HighAvailability bean = ha( db );

            assertEquals( 3, bean.getInstancesInCluster().length );
            for ( ClusterMemberInfo info : bean.getInstancesInCluster() )
            {
                assertTrue( "every instance should be available", info.isAvailable() );
                if ( info.getClusterRoles().length > 0 )
                {
                    assertEquals(ClusterConfiguration.COORDINATOR, info.getClusterRoles()[0] );
                    assertEquals( HighAvailabilityMemberState.MASTER.name(), info.getHaRole() );
                }
                else
                {
                    assertEquals( "instance "+info.getClusterId()+" is cluster slave but HA master",
                            HighAvailabilityMemberState.SLAVE.name(), info.getHaRole() );
                }
            }
        }
    }

    @Test
    public void testAfterHardMasterSwitchClusterInfoIsCorrect() throws Throwable
    {
        startCluster( 3 );
        cluster.fail( cluster.getMaster() );
        RepairKit masterShutdown = cluster.shutdown( cluster.getMaster() );
        cluster.await( ClusterManager.masterAvailable() );
        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            assertEquals( 3, ha( db ).getInstancesInCluster().length );
        }
        assertEquals( "2", ha( cluster.getMaster()).getInstanceId());
        masterShutdown.repair();
        cluster.await( ClusterManager.masterAvailable() );
        cluster.await( ClusterManager.masterSeesAllSlavesAsAvailable() );
        assertEquals( "2", ha( cluster.getMaster()).getInstanceId());
        assertFalse( cluster.getMemberByServerId( 1 ).isMaster() );
        Thread.sleep(1000);
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            HighAvailability bean = ha( db );

            assertEquals( 3, bean.getInstancesInCluster().length );
            for ( ClusterMemberInfo info : bean.getInstancesInCluster() )
            {
                assertTrue( bean.getInstanceId() + ": every instance should be available: "+info.getClusterId(),
                        info.isAvailable() );
                if ( info.getClusterRoles().length > 0 )
                {
                    assertEquals( bean.getInstanceId() + ": Got a role different than coordinator?",
                            ClusterConfiguration.COORDINATOR, info.getClusterRoles()[0] );
                    assertEquals( bean.getInstanceId()+": got a coordinator who is not master: "+info.getClusterId(),
                            HighAvailabilityMemberState.MASTER.name(), info.getHaRole() );
                }
                else
                {
                    assertEquals( bean.getInstanceId()+": instance "+info.getClusterId()+" is cluster slave but HA master",
                            HighAvailabilityMemberState.SLAVE.name(), info.getHaRole() );
                }
            }
        }
    }

    private void assertMasterInformation( HighAvailability ha )
    {
        assertEquals( "instance id should be the one configured", "1", ha.getInstanceId() );
        assertTrue( "single instance should be master and available", ha.isAvailable() );
        assertEquals( "single instance should be master", HighAvailabilityMemberState.MASTER.name(), ha.getRole() );
        ClusterMemberInfo info = ha.getInstancesInCluster()[0];
        assertTrue( "single instance should be the returned instance id", info.getClusterId().endsWith( ":5001" ) );
        assertTrue( "single instance should have coordinator cluster role", Arrays.equals( info.getClusterRoles(),
                new String[]{ClusterConfiguration.COORDINATOR} ) );
    }

    @Test
    public void canGetBranchedStoreBean() throws Throwable
    {
        startCluster( 1 );
        BranchedStore bs = beans( cluster.getMaster() ).getBranchedStoreBean();
        assertNotNull( "could not get branched store bean", bs );
        assertEquals( "no branched stores for new db", 0,
                bs.getBranchedStores().length );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canGetInstanceConnectionInformation() throws Throwable
    {
        startCluster( 1 );
        ClusterMemberInfo[] clusterMembers = ha( cluster.getMaster() ).getInstancesInCluster();
        assertNotNull( clusterMembers );
        assertEquals( 1, clusterMembers.length );
        ClusterMemberInfo clusterMember = clusterMembers[0];
        assertNotNull( clusterMember );
//        String address = clusterMember.getAddress();
//        assertNotNull( "No JMX address for instance", address );
        String id = clusterMember.getClusterId();
        assertNotNull( "No instance id", id );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canConnectToInstance() throws Throwable
    {
        startCluster( 1 );
        ClusterMemberInfo[] clusterMembers = ha( cluster.getMaster() ).getInstancesInCluster();
        assertNotNull( clusterMembers );
        assertEquals( 1, clusterMembers.length );
        ClusterMemberInfo clusterMember = clusterMembers[0];
        assertNotNull( clusterMember );
        Pair<Neo4jManager, HighAvailability> proc = clusterMember.connect();
        assertNotNull( "could not connect", proc );
        Neo4jManager neo4j = proc.first();
        HighAvailability ha = proc.other();
        assertNotNull( neo4j );
        assertNotNull( ha );

        clusterMembers = ha.getInstancesInCluster();
        assertNotNull( clusterMembers );
        assertEquals( 1, clusterMembers.length );
//        assertEquals( clusterMember.getAddress(), clusterMembers[0].getAddress() );
        assertEquals( clusterMember.getClusterId(), clusterMembers[0].getClusterId() );
    }
    
    @Test
    public void joinedInstanceShowsUpAsSlave() throws Throwable
    {
        startCluster( 2 );
        ClusterMemberInfo[] instancesInCluster = ha( cluster.getMaster() ).getInstancesInCluster();
        assertEquals( 2, instancesInCluster.length );
        ClusterMemberInfo[] secondInstancesInCluster = ha( cluster.getAnySlave() ).getInstancesInCluster();
        assertEquals( 2, secondInstancesInCluster.length );
        assertMasterAndSlaveInformation( instancesInCluster );
        assertMasterAndSlaveInformation( secondInstancesInCluster );
    }
    
    @Test
    public void leftInstanceDisappearsFromMemberList() throws Throwable
    {
        // Start the second db and make sure it's visible in the member list.
        // Then shut it down to see if it disappears from the member list again.
        startCluster( 2 );
        assertEquals( 2, ha( cluster.getAnySlave() ).getInstancesInCluster().length );
        cluster.shutdown( cluster.getAnySlave() );
        
        assertEquals( 1, ha( cluster.getMaster() ).getInstancesInCluster().length );
        assertMasterInformation( ha( cluster.getMaster() ) );
    }
    
    @Test
    public void failedMemberIsStillInMemberListAlthoughFailed() throws Throwable
    {
        startCluster( 3 );
        assertEquals( 3, ha( cluster.getAnySlave() ).getInstancesInCluster().length );
        
        // Fail the instance
        HighlyAvailableGraphDatabase failedDb = cluster.getAnySlave();
        RepairKit dbFailure = cluster.fail( failedDb );
        await( ha( cluster.getMaster() ), dbAlive( false ) );
        await( ha( cluster.getAnySlave( failedDb )), dbAlive( false ) );
        // Repair the failure and come back
        dbFailure.repair();
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
            await( ha( db ), dbAvailability( true ) );
    }
    
    private void assertMasterAndSlaveInformation( ClusterMemberInfo[] instancesInCluster ) throws Exception
    {
        ClusterMemberInfo master = member( instancesInCluster, 5001 );
        assertTrue( master.getClusterId().endsWith( ":5001" ) );
        assertEquals( HighAvailabilityMemberState.MASTER.name(), master.getHaRole() );
        assertTrue( "Unexpected start of HA URI " + uri( "ha", master.getUris() ),
                uri( "ha", master.getUris() ).startsWith( "ha://" + InetAddress.getLocalHost().getHostAddress() + ":1137" ) );
        assertTrue( "Master not available", master.isAvailable() );

        ClusterMemberInfo slave = member( instancesInCluster, 5002 );
        assertTrue( slave.getClusterId().endsWith( ":5002" ) );
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

    private void await( HighAvailability ha, Predicate<ClusterMemberInfo> predicate ) throws InterruptedException
    {
        long end = System.currentTimeMillis() + SECONDS.toMillis( 300 );
        boolean conditionMet = false;
        while ( System.currentTimeMillis() < end )
        {
            conditionMet = predicate.accept( member( ha.getInstancesInCluster(), 5002 ) ); 
            if ( conditionMet )
                return;
            Thread.sleep( 500 );
        }
        fail( "Failed instance didn't show up as such in JMX" );
    }

    private Predicate<ClusterMemberInfo> dbAvailability( final boolean available )
    {
        return new Predicate<ClusterMemberInfo>()
        {
            @Override
            public boolean accept( ClusterMemberInfo item )
            {
                return item.isAvailable() == available;
            }
        };
    }

    private Predicate<ClusterMemberInfo> dbAlive( final boolean alive )
    {
        return new Predicate<ClusterMemberInfo>()
        {
            @Override
            public boolean accept( ClusterMemberInfo item )
            {
                return item.isAlive() == alive;
            }
        };
    }
}
