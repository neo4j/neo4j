/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.ha.management;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.function.Predicate;
import javax.management.NotCompliantMBeanException;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.ManagementSupport;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.LastUpdateTime;
import org.neo4j.kernel.ha.UpdatePuller;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.impl.core.LastTxIdGetter;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.internal.KernelData;
import org.neo4j.kernel.internal.Version;
import org.neo4j.management.ClusterMemberInfo;
import org.neo4j.management.HighAvailability;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.MASTER;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.SLAVE;
import static org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher.UNKNOWN;
import static org.neo4j.kernel.impl.store.StoreId.DEFAULT;

public class HighAvailabilityBeanTest
{
    private final GraphDatabaseAPI db = mock( HighlyAvailableGraphDatabase.class );
    private final Dependencies dependencies = new Dependencies();
    private final ClusterMembers clusterMembers = mock( ClusterMembers.class );
    private final HighAvailabilityBean bean = new HighAvailabilityBean();
    private final LastTxIdGetter lastTxIdGetter = mock( LastTxIdGetter.class );
    private final LastUpdateTime lastUpdateTime = mock( LastUpdateTime.class );
    private final ClusterDatabaseInfoProvider dbInfoProvider =
            new ClusterDatabaseInfoProvider( clusterMembers, lastTxIdGetter, lastUpdateTime );
    private DefaultFileSystemAbstraction fileSystem;
    private KernelData kernelData;
    private HighAvailability haBean;

    @Before
    public void setup() throws NotCompliantMBeanException
    {
        fileSystem = new DefaultFileSystemAbstraction();
        kernelData = new TestHighlyAvailableKernelData();
        ManagementData data = new ManagementData( bean, kernelData, ManagementSupport.load() );

        when( db.getDependencyResolver() ).thenReturn( dependencies );
        haBean = (HighAvailability) new HighAvailabilityBean().createMBean( data );
    }

    @After
    public void tearDown() throws IOException
    {
        kernelData.shutdown();
        fileSystem.close();
    }

    @Test
    public void shouldPickUpOnLastCommittedTxId() throws Exception
    {
        // GIVEN
        long txId = 101L;
        when( lastTxIdGetter.getLastTxId() ).thenReturn( txId, txId + 1 );
        when( clusterMembers.getCurrentMember() ).thenReturn( clusterMember( 1, MASTER, 1010 ) );

        // WHEN
        long reportedTxId = haBean.getLastCommittedTxId();

        // THEN
        assertEquals( txId, reportedTxId );

        // and WHEN
        long nextReportedTxId = haBean.getLastCommittedTxId();

        // THEN
        assertEquals( txId + 1, nextReportedTxId );
    }

    @Test
    public void shouldPickUpOnLastUpdateTime() throws Exception
    {
        // GIVEN
        long updateTime = 123456789L;
        when( lastUpdateTime.getLastUpdateTime() ).thenReturn( 0L, updateTime, updateTime + 1_000 );
        when( clusterMembers.getCurrentMember() ).thenReturn( clusterMember( 1, MASTER, 1010 ) );

        // WHEN
        String reportedUpdateTime = haBean.getLastUpdateTime();

        // THEN
        assertEquals( "N/A", reportedUpdateTime );

        // and WHEN
        String secondReportedUpdateTime = haBean.getLastUpdateTime();

        // THEN
        assertEquals( Format.date( updateTime ), secondReportedUpdateTime );

        // and WHEN
        String thirdReportedTxId = haBean.getLastUpdateTime();

        // THEN
        assertEquals( Format.date( updateTime + 1_000 ), thirdReportedTxId );
    }

    @Test
    public void shouldSeeChangesInClusterMembers() throws Exception
    {
        // GIVEN
        when( clusterMembers.getMembers() ).thenReturn( asList(
                clusterMember( 1, MASTER, 1137 ),
                clusterMember( 2, SLAVE, 1138 ),
                clusterMember( 3, SLAVE, 1139 ) ) );

        // THEN
        assertMasterAndSlaveInformation( haBean.getInstancesInCluster() );

        // and WHEN
        when( clusterMembers.getMembers() ).thenReturn( asList(
                clusterMember( 1, SLAVE, 1137 ),
                clusterMember( 2, MASTER, 1138 ),
                clusterMember( 3, SLAVE, 1139 ) ) );

        // THEN
        for ( ClusterMemberInfo info : haBean.getInstancesInCluster() )
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

    @Test
    public void shouldSeeLeavingMemberDisappear() throws Exception
    {
        // GIVEN
        when( clusterMembers.getMembers() ).thenReturn( asList(
                clusterMember( 1, MASTER, 1137 ),
                clusterMember( 2, SLAVE, 1138 ),
                clusterMember( 3, SLAVE, 1139 ) ) );
        assertMasterAndSlaveInformation( haBean.getInstancesInCluster() );

        // WHEN
        when( clusterMembers.getMembers() ).thenReturn( asList(
                clusterMember( 1, MASTER, 1137 ),
                clusterMember( 3, SLAVE, 1139 ) ) );

        // THEN
        assertEquals( 2, haBean.getInstancesInCluster().length );
    }

    @Test
    public void shouldSeeFailedMembersInMemberList() throws Exception
    {
        // GIVEN
        when( clusterMembers.getMembers() ).thenReturn( asList(
                clusterMember( 1, MASTER, 1137 ),
                clusterMember( 2, SLAVE, 1138 ),
                clusterMember( 3, UNKNOWN, 1139, false ) ) );

        // WHEN
        ClusterMemberInfo[] instances = haBean.getInstancesInCluster();

        // THEN
        assertEquals( 3, instances.length );
        assertEquals( 2, count( instances, ClusterMemberInfo::isAlive ) );
        assertEquals( 2, count( instances, ClusterMemberInfo::isAvailable ) );
    }

    @Test
    public void shouldPullUpdates() throws Exception
    {
        // GIVEN
        UpdatePuller updatePuller = mock( UpdatePuller.class );
        dependencies.satisfyDependency( updatePuller );

        // WHEN
        String result = haBean.update();

        // THEN
        verify( updatePuller ).pullUpdates();
        assertTrue( result, result.contains( "Update completed in" ) );
    }

    @Test
    public void shouldReportFailedPullUpdates() throws Exception
    {
        // GIVEN
        UpdatePuller updatePuller = mock( UpdatePuller.class );
        RuntimeException myException = new RuntimeException( "My test exception" );
        Mockito.doThrow( myException ).when( updatePuller ).pullUpdates();
        dependencies.satisfyDependency( updatePuller );

        // WHEN
        String result = haBean.update();

        // THEN
        verify( updatePuller ).pullUpdates();
        assertTrue( result, result.contains( myException.getMessage() ) );
    }

    private int count( ClusterMemberInfo[] instances, Predicate<ClusterMemberInfo> filter )
    {
        int count = 0;
        for ( ClusterMemberInfo instance : instances )
        {
            if ( filter.test( instance ) )
            {
                count++;
            }
        }
        return count;
    }

    private ClusterMember clusterMember( int serverId, String role, int port ) throws URISyntaxException
    {
        return clusterMember( serverId, role, port, true );
    }

    private ClusterMember clusterMember( int serverId, String role, int port, boolean alive ) throws URISyntaxException
    {
        URI uri = HighAvailabilityModeSwitcher.UNKNOWN.equals( role ) ? null : new URI( "ha://" + role + ":" + port );
        return new ClusterMember( new InstanceId( serverId ),
                MapUtil.genericMap( role, uri ), DEFAULT, alive );
    }

    private void assertMasterAndSlaveInformation( ClusterMemberInfo[] instancesInCluster )
    {
        ClusterMemberInfo master = member( instancesInCluster, 1 );
        assertEquals( 1137,
                getUriForScheme( "ha", Iterables.map( URI::create, Arrays.asList( master.getUris() ) ) ).getPort() );
        assertEquals( HighAvailabilityModeSwitcher.MASTER, master.getHaRole() );
        ClusterMemberInfo slave = member( instancesInCluster, 2 );
        assertEquals( 1138,
                getUriForScheme( "ha", Iterables.map( URI::create, Arrays.asList( slave.getUris() ) ) ).getPort() );
        assertEquals( HighAvailabilityModeSwitcher.SLAVE, slave.getHaRole() );
        assertTrue( "Slave not available", slave.isAvailable() );
    }

    private static URI getUriForScheme( final String scheme, Iterable<URI> uris )
    {
        return firstOrNull( filter( item ->  item.getScheme().equals( scheme ), uris ) );
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

    private class TestHighlyAvailableKernelData extends HighlyAvailableKernelData
    {
        TestHighlyAvailableKernelData()
        {
            super( HighAvailabilityBeanTest.this.db, HighAvailabilityBeanTest.this.clusterMembers,
                    HighAvailabilityBeanTest.this.dbInfoProvider, HighAvailabilityBeanTest.this.fileSystem, null,
                    new File( "storeDir" ), Config.defaults() );
        }

        @Override
        public Version version()
        {
            return Version.getKernel();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return db;
        }
    }
}
