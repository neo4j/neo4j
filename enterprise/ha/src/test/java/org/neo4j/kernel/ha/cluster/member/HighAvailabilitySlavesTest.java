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
package org.neo4j.kernel.ha.cluster.member;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.master.DefaultSlaveFactory;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveFactory;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.ReflectionUtil;

import static java.net.URI.create;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher.SLAVE;

public class HighAvailabilitySlavesTest
{
    private static final InstanceId INSTANCE_ID = new InstanceId( 1 );
    private static final URI HA_URI = create( "ha://server1?serverId=" + INSTANCE_ID.toIntegerIndex() );
    private static final URI CLUSTER_URI = create( "cluster://server2" );

    @Test
    public void shouldRegisterItselfOnMonitors()
    {
        // given
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        Cluster cluster = mock( Cluster.class );
        SlaveFactory slaveFactory = mock( SlaveFactory.class );

        // when
        new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory, new HostnamePort( null, 0 ) ).init();

        // then
        verify( cluster ).addClusterListener( any( ClusterListener.class ) );
    }

    @Test
    public void shouldNotReturnUnavailableSlaves()
    {
        // given
        Cluster cluster = mock( Cluster.class );
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        when( clusterMembers.getAliveMembers() ).thenReturn( Iterables.option( new ClusterMember( INSTANCE_ID ) ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory,
                new HostnamePort( null, 0 ) );
        slaves.init();

        // when
        Iterable<Slave> memberSlaves = slaves.getSlaves();

        // then
        assertThat( count( memberSlaves ), equalTo( 0L ) );
    }

    @Test
    public void shouldReturnAvailableAndAliveSlaves()
    {
        // given
        Cluster cluster = mock( Cluster.class );
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        when( clusterMembers.getAliveMembers() ).thenReturn( Iterables.option(
                new ClusterMember( INSTANCE_ID ).availableAs( SLAVE, HA_URI, StoreId.DEFAULT ) ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        when( slaveFactory.newSlave( any( LifeSupport.class ), any( ClusterMember.class ), any( String.class ),
                any( Integer.class ) ) ).thenReturn( mock( Slave.class ) );

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory,
                new HostnamePort( null, 0 ) );
        slaves.init();

        // when
        Iterable<Slave> memberSlaves = slaves.getSlaves();

        // then
        assertThat( count( memberSlaves ), equalTo( 1L ) );
    }

    @Test
    public void shouldClearSlavesWhenNewMasterElected()
    {
        // given
        Cluster cluster = mock( Cluster.class );
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        when( clusterMembers.getAliveMembers() ).thenReturn( Iterables.option(
                new ClusterMember( INSTANCE_ID ).availableAs( SLAVE, HA_URI, StoreId.DEFAULT ) ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        when( slaveFactory.newSlave( any( LifeSupport.class ), any( ClusterMember.class ), any( String.class ),
                any( Integer.class ) ) ).thenReturn( mock( Slave.class ), mock( Slave.class ) );

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory, new
                HostnamePort( null, 0 ) );
        slaves.init();

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );

        // when
        Slave slave1 = slaves.getSlaves().iterator().next();

        listener.getValue().elected( ClusterConfiguration.COORDINATOR, INSTANCE_ID, CLUSTER_URI );

        Slave slave2 = slaves.getSlaves().iterator().next();

        // then
        assertThat( slave2, not( sameInstance( slave1 ) ) );
    }

    @Test
    public void shouldSupportConcurrentConsumptionOfSlaves() throws Exception
    {
        // Given
        HighAvailabilitySlaves haSlaves = new HighAvailabilitySlaves( clusterMembersOfSize( 1000 ),
                mock( Cluster.class ), new DefaultSlaveFactory( NullLogProvider.getInstance(), new Monitors(), 42 ),
                        new HostnamePort( null, 0 ) );

        // When
        ExecutorService executor = Executors.newFixedThreadPool( 5 );
        for ( int i = 0; i < 5; i++ )
        {
            executor.submit( slavesConsumingRunnable( haSlaves ) );
        }
        executor.shutdown();
        executor.awaitTermination( 30, SECONDS );

        // Then
        int slavesCount = 0;
        LifeSupport life = ReflectionUtil.getPrivateField( haSlaves, "life", LifeSupport.class );
        for ( Lifecycle lifecycle : life.getLifecycleInstances() )
        {
            if ( lifecycle instanceof Slave )
            {
                slavesCount++;
            }
        }
        assertEquals( "Unexpected number of slaves", 1000 - 1, slavesCount ); // One instance is master
    }

    private static ClusterMembers clusterMembersOfSize( int size )
    {
        List<ClusterMember> members = new ArrayList<>( size );
        members.add( mockClusterMemberWithRole( HighAvailabilityModeSwitcher.MASTER ) );
        for ( int i = 0; i < size - 1; i++ )
        {
            members.add( mockClusterMemberWithRole( SLAVE ) );
        }

        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        when( clusterMembers.getAliveMembers() ).thenReturn( members );

        return clusterMembers;
    }

    private static ClusterMember mockClusterMemberWithRole( String role )
    {
        ClusterMember member = mock( ClusterMember.class );
        when( member.getHAUri() ).thenReturn( URI.create( "http://localhost:7474" ) );
        when( member.isAlive() ).thenReturn( true );
        when( member.hasRole( eq( role ) ) ).thenReturn( true );
        return member;
    }

    private static Runnable slavesConsumingRunnable( final HighAvailabilitySlaves haSlaves )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                for ( Slave slave : haSlaves.getSlaves() )
                {
                    assertNotNull( slave );
                }
            }
        };
    }
}
