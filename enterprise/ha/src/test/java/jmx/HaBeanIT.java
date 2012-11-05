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

import java.lang.reflect.Field;
import java.net.InetAddress;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.com.NetworkInstance;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Predicate;
import org.neo4j.jmx.Kernel;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.management.BranchedStore;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;
import org.neo4j.management.Neo4jManager;
import org.neo4j.test.TargetDirectory;

public class HaBeanIT
{
    private static final TargetDirectory dir = TargetDirectory.forTest( HaBeanIT.class );
    private static final Db db1 = new Db( 1 ), db2 = new Db( 2 ), db3 = new Db( 3 );
    
    private static class Db
    {
        private final int id;
        private final String storeDir;
        private HighlyAvailableGraphDatabase db;
        
        Db( int id )
        {
            this.id = id;
            storeDir = dir.directory( "" + id, true ).getAbsolutePath();
        }
        
        public Neo4jManager beans()
        {
            return new Neo4jManager( db.getDependencyResolver().resolveDependency( JmxKernelExtension
                    .class ).getSingleManagementBean( Kernel.class ) );
        }
        
        public HighAvailability ha()
        {
            return beans().getHighAvailabilityBean();
        }
        
        public void start()
        {
            if ( db != null )
                return;
            
            db = (HighlyAvailableGraphDatabase) new HighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( storeDir )
                .setConfig( HaSettings.server_id, "" + id )
                .setConfig( "jmx.port", "" + (9912 + id) )
                .setConfig( HaSettings.ha_server, ":" + (1136 + id) )
                .setConfig( HaSettings.initial_hosts, ":5001" )
                .newGraphDatabase();
        }
        
        public void fail() throws Exception
        {
            ClusterClient clusterClient = db.getDependencyResolver().resolveDependency( ClusterClient.class );
            LifeSupport clusterClientLife = (LifeSupport) accessible( clusterClient.getClass().getDeclaredField( "life" ) ).get( clusterClient );
            clusterClientLife.remove( instance( NetworkInstance.class, clusterClientLife.getLifecycleInstances() ) );
            shutdown();
        }
        
        public void repair() throws Exception
        {
            start();
        }
        
        public void shutdown()
        {
            if ( db != null )
            {
                db.shutdown();
                db = null;
            }
        }
    }

    @BeforeClass
    public static void startDb() throws Exception
    {
        db1.start();
    }

    @AfterClass
    public static void stopDb() throws Exception
    {
        db1.shutdown();
        db2.shutdown();
        db3.shutdown();
        dir.cleanup();
    }
    
    private static <T> T instance( Class<T> classToFind, Iterable<?> from )
    {
        for ( Object item : from )
            if ( classToFind.isAssignableFrom( item.getClass() ) )
                return (T) item;
        fail( "Couldn't find the network instance to fail. Internal field, so fragile sensitive to changes though" );
        return null; // it will never get here.
    }

    private static Field accessible( Field field )
    {
        field.setAccessible( true );
        return field;
    }

    @Test
    public void canGetHaBean() throws Exception
    {
        assertNotNull( "could not get ha bean", db1.ha() );
        assertMasterInformation( db1.ha() );
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
        BranchedStore bs = db1.beans().getBranchedStoreBean();
        assertNotNull( "could not get ha bean", bs );
        assertEquals( "no branched stores for new db", 0,
                bs.getBranchedStores().length );
    }

    @Test
    @Ignore //Temporary ignore since this doesn't work well on Linux 2011-04-08
    public void canGetInstanceConnectionInformation() throws Exception
    {
        ClusterMemberInfo[] clusterMembers = db1.ha().getInstancesInCluster();
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
        ClusterMemberInfo[] clusterMembers = db1.ha().getInstancesInCluster();
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
        assertEquals( clusterMember.getInstanceId(), clusterMembers[0].getInstanceId() );
    }
    
    @Test
    public void joinedInstanceShowsUpAsSlave() throws Exception
    {
        db2.start();
        ClusterMemberInfo[] instancesInCluster = db1.ha().getInstancesInCluster();
        assertEquals( 2, instancesInCluster.length );
        ClusterMemberInfo[] secondInstancesInCluster = db2.ha().getInstancesInCluster();
        assertEquals( 2, secondInstancesInCluster.length );
        
        assertMasterAndSlaveInformation( instancesInCluster );
        assertMasterAndSlaveInformation( secondInstancesInCluster );
    }
    
    @Test
    public void leftInstanceDisappearsFromMemberList() throws Exception
    {
        // Start the second db and make sure it's visible in the member list.
        // Then shut it down to see if it disappears from the member list again.
        db2.start();
        assertEquals( 2, db2.ha().getInstancesInCluster().length );
        db2.shutdown();
        
        assertEquals( 1, db1.ha().getInstancesInCluster().length );
        assertMasterInformation( db1.ha() );
    }
    
    @Test
    public void failedMemberIsStillInMemberListAlthoughUnavailable() throws Exception
    {
        db2.start();
        db3.start();
        assertEquals( 3, db2.ha().getInstancesInCluster().length );
        
        // Fail the instance
        db2.fail();
        await( db1.ha(), dbAvailability( false ) );
        await( db3.ha(), dbAvailability( false ) );
        
        // Repair the failure and come back
        db2.repair();
        await( db1.ha(), dbAvailability( true ) );
        await( db3.ha(), dbAvailability( true ) );
        await( db2.ha(), dbAvailability( true ) );
        db3.shutdown();
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
}
