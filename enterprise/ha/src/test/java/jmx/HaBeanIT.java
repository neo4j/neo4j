/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.test.ha.ClusterManager.RepairKit;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterSeesMembers;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

public class HaBeanIT
{
    @Rule
    public final TestName testName = new TestName();

    private static final TargetDirectory dir = TargetDirectory.forTest( HaBeanIT.class );
    private ManagedCluster cluster;
    private ClusterManager clusterManager;

    public void startCluster( int size ) throws Throwable
    {
        clusterManager = new ClusterManager( clusterOfSize( size ), dir.cleanDirectory( testName.getMethodName() ), MapUtil.stringMap() )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, InstanceId serverId )
            {
                builder.setConfig( "jmx.port", "" + ( 9912 + serverId.toIntegerIndex() ) );
                builder.setConfig( HaSettings.ha_server, ":" + ( 1136 + serverId.toIntegerIndex() ) );
                builder.setConfig( GraphDatabaseSettings.forced_kernel_id, testName.getMethodName() + serverId );

            }
        };
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( ClusterManager.allSeesAllAsAvailable() );
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

    private void assertMasterInformation( HighAvailability ha )
    {
        assertTrue( "single instance should be master and available", ha.isAvailable() );
        assertEquals( "single instance should be master", HighAvailabilityModeSwitcher.MASTER, ha.getRole() );
        ClusterMemberInfo info = ha.getInstancesInCluster()[0];
        assertEquals( "single instance should be the returned instance id", "1", info.getInstanceId() );
    }

    @Test
    public void testLatestTxInfoIsCorrect() throws Throwable
    {
        startCluster( 1 );
        HighlyAvailableGraphDatabase db = cluster.getMaster();
        HighAvailability masterHa = ha( db );
        long lastCommitted = masterHa.getLastCommittedTxId();
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }
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
        cluster.await( ClusterManager.allSeesAllAsAvailable() );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            HighAvailability bean = ha( db );

            assertEquals( 3, bean.getInstancesInCluster().length );
            for ( ClusterMemberInfo info : bean.getInstancesInCluster() )
            {
                assertTrue( "every instance should be available", info.isAvailable() );
                assertTrue( "every instances should have at least one role", info.getRoles().length > 0 );
                if ( HighAvailabilityModeSwitcher.MASTER.equals( info.getRoles()[0] ) )
                {
                    assertEquals( "coordinator should be master",
                            HighAvailabilityModeSwitcher.MASTER, info.getHaRole() );
                }
                else
                {
                    assertEquals( "Either master or slave, no other way",
                            HighAvailabilityModeSwitcher.SLAVE, info.getRoles()[0] );
                    assertEquals( "instance " + info.getInstanceId() + " is cluster slave but HA master",
                            HighAvailabilityModeSwitcher.SLAVE, info.getHaRole() );
                }
                for ( String uri : info.getUris() )
                {
                    assertTrue( "roles should contain URIs",
                            uri.startsWith( "ha://" ) || uri.startsWith( "backup://" ) );
                }
            }
        }
    }

    @Test
    public void testAfterHardMasterSwitchClusterInfoIsCorrect() throws Throwable
    {
        startCluster( 3 );
        RepairKit masterShutdown = cluster.fail( cluster.getMaster() );
        cluster.await( ClusterManager.masterAvailable() );
        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 1 ) );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            if ( db.getInstanceState() == HighAvailabilityMemberState.PENDING )
            {
                continue;
            }
            // Instance that was hard killed will still be in the cluster
            assertEquals( 3, ha( db ).getInstancesInCluster().length );
        }
        masterShutdown.repair();
        cluster.await( ClusterManager.masterAvailable() );
        cluster.await( ClusterManager.masterSeesSlavesAsAvailable( 2 ) );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            int mastersFound = 0;
            HighAvailability bean = ha( db );

            assertEquals( 3, bean.getInstancesInCluster().length );
            for ( ClusterMemberInfo info : bean.getInstancesInCluster() )
            {
                assertTrue( bean.getInstanceId() + ": every instance should be available: " + info.getInstanceId(),
                        info.isAvailable() );
                for ( String role : info.getRoles() )
                {
                    if (role.equals( HighAvailabilityModeSwitcher.MASTER ))
                    {
                        mastersFound++;
                    }
                }
            }
            assertEquals( 1, mastersFound );
        }
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
        startCluster( 3 );
        assertEquals( 3, ha( cluster.getAnySlave() ).getInstancesInCluster().length );
        cluster.shutdown( cluster.getAnySlave() );

        cluster.await( masterSeesMembers( 2 ) );

        assertEquals( 2, ha( cluster.getMaster() ).getInstancesInCluster().length );
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
        {
            await( ha( db ), dbAvailability( true ) );
            await( ha( db ), dbAlive( true ) );
        }
    }

    public static URI getUriForScheme( final String scheme, Iterable<URI> uris )
    {
        return first( filter( new Predicate<URI>()
        {
            @Override
            public boolean accept( URI item )
            {
                return item.getScheme().equals( scheme );
            }
        }, uris ) );
    }

    private void assertMasterAndSlaveInformation( ClusterMemberInfo[] instancesInCluster ) throws Exception
    {
        ClusterMemberInfo master = member( instancesInCluster, 1 );
        assertEquals( 1137, getUriForScheme( "ha", Iterables.map( new Function<String, URI>()
        {
            @Override
            public URI apply( String from )
            {
                return URI.create( from );
            }
        }, Arrays.asList( master.getUris() ) ) ).getPort() );
        assertEquals( HighAvailabilityModeSwitcher.MASTER, master.getHaRole() );

        ClusterMemberInfo slave = member( instancesInCluster, 2 );
        assertEquals( 1138, getUriForScheme( "ha", Iterables.map( new Function<String, URI>()
        {
            @Override
            public URI apply( String from )
            {
                return URI.create( from );
            }
        }, Arrays.asList( slave.getUris() ) ) ).getPort() );
        assertEquals( HighAvailabilityModeSwitcher.SLAVE, slave.getHaRole() );
        assertTrue( "Slave not available", slave.isAvailable() );
    }

    private ClusterMemberInfo member( ClusterMemberInfo[] members, int instanceId )
    {
        for ( ClusterMemberInfo member : members )
        {
            if ( member.getInstanceId().equals( Integer.toString( instanceId ) ) )
            {
                return member;
            }
        }
        fail( "Couldn't find cluster member with cluster URI port " + instanceId + " among " + Arrays.toString(
                members ) );
        return null; // it will never get here.
    }

    private void await( HighAvailability ha, Predicate<ClusterMemberInfo> predicate ) throws InterruptedException
    {
        long end = System.currentTimeMillis() + SECONDS.toMillis( 300 );
        boolean conditionMet = false;
        while ( System.currentTimeMillis() < end )
        {
            conditionMet = predicate.accept( member( ha.getInstancesInCluster(), 2 ) );
            if ( conditionMet )
            {
                return;
            }
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
