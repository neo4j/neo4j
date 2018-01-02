/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import org.neo4j.function.Function;
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.ha.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.first;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.setting;
import static org.neo4j.kernel.impl.ha.ClusterManager.instanceEvicted;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesMembers;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;
import static org.neo4j.test.ha.ClusterRule.intBase;
import static org.neo4j.test.ha.ClusterRule.stringWithIntBase;

public class HaBeanIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withInstanceSetting( setting( "jmx.port", STRING, (String) null ), intBase( 9912 ) )
            .withInstanceSetting( HaSettings.ha_server, stringWithIntBase( ":", 1136 ) )
            .withInstanceSetting( GraphDatabaseSettings.forced_kernel_id, stringWithIntBase( "kernel", 0 ) );

    @Test
    public void canGetHaBean() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        HighAvailability ha = ha( cluster.getMaster() );
        assertNotNull( "could not get ha bean", ha );
        assertMasterInformation( ha );
    }

    private void assertMasterInformation( HighAvailability ha )
    {
        assertTrue( "should be available", ha.isAvailable() );
        assertEquals( "should be master", HighAvailabilityModeSwitcher.MASTER, ha.getRole() );
    }

    @Test
    public void testLatestTxInfoIsCorrect() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
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
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        try (Transaction tx = master.beginTx())
        {
            master.createNode();
            tx.success();
        }
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
        ManagedCluster cluster = clusterRule.startCluster();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        RepairKit masterShutdown = cluster.shutdown( master );

        cluster.await( masterAvailable( master ) );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );

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
        ManagedCluster cluster = clusterRule.startCluster();

        cluster.await( masterSeesSlavesAsAvailable( 2 ) );

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        RepairKit masterShutdown = cluster.fail( master );

        cluster.await( instanceEvicted( master ) );

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
                    if ( role.equals( HighAvailabilityModeSwitcher.MASTER ) )
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
        ManagedCluster cluster = clusterRule.startCluster();
        BranchedStore bs = beans( cluster.getMaster() ).getBranchedStoreBean();
        assertNotNull( "could not get branched store bean", bs );
    }

    @Test
    public void joinedInstanceShowsUpAsSlave() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        ClusterMemberInfo[] instancesInCluster = ha( cluster.getMaster() ).getInstancesInCluster();
        assertEquals( 3, instancesInCluster.length );
        ClusterMemberInfo[] secondInstancesInCluster = ha( cluster.getAnySlave() ).getInstancesInCluster();
        assertEquals( 3, secondInstancesInCluster.length );
        assertMasterAndSlaveInformation( instancesInCluster );
        assertMasterAndSlaveInformation( secondInstancesInCluster );
    }

    @Test
    public void leftInstanceDisappearsFromMemberList() throws Throwable
    {
        // Start the cluster and make sure it's up.
        // Then shut down one of the slaves to see if it disappears from the member list.
        ManagedCluster cluster = clusterRule.startCluster();
        assertEquals( 3, ha( cluster.getAnySlave() ).getInstancesInCluster().length );
        RepairKit repair = cluster.shutdown( cluster.getAnySlave() );

        try
        {
            cluster.await( masterSeesMembers( 2 ) );
            HighAvailability haMaster = ha( cluster.getMaster() );
            assertEquals( 2, haMaster.getInstancesInCluster().length );
        }
        finally
        {
            repair.repair();
        }
    }

    @Test
    public void failedMemberIsStillInMemberListAlthoughFailed() throws Throwable
    {
        ManagedCluster cluster = clusterRule.startCluster();
        assertEquals( 3, ha( cluster.getAnySlave() ).getInstancesInCluster().length );

        // Fail the instance
        HighlyAvailableGraphDatabase failedDb = cluster.getAnySlave();
        RepairKit dbFailure = cluster.fail( failedDb );
        try
        {
            await( ha( cluster.getMaster() ), dbAlive( false ) );
            await( ha( cluster.getAnySlave( failedDb ) ), dbAlive( false ) );
        }
        finally
        {
            // Repair the failure and come back
            dbFailure.repair();
        }
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            await( ha( db ), dbAvailability( true ) );
            await( ha( db ), dbAlive( true ) );
        }
    }

    private Neo4jManager beans( HighlyAvailableGraphDatabase db )
    {
        return new Neo4jManager( db.getDependencyResolver().resolveDependency( JmxKernelExtension
                .class ).getSingleManagementBean( Kernel.class ) );
    }

    private HighAvailability ha( HighlyAvailableGraphDatabase db )
    {
        return beans( db ).getHighAvailabilityBean();
    }

    private static URI getUriForScheme( final String scheme, Iterable<URI> uris )
    {
        return first( filter( new Predicate<URI>()
        {
            @Override
            public boolean test( URI item )
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
        while ( System.currentTimeMillis() < end )
        {
            if ( predicate.test( member( ha.getInstancesInCluster(), 2 ) ) )
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
            public boolean test( ClusterMemberInfo item )
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
            public boolean test( ClusterMemberInfo item )
            {
                return item.isAlive() == alive;
            }
        };
    }
}
