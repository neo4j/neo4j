/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static java.net.URI.create;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.count;

import java.net.URI;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.cluster.Cluster;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.com.master.Slave;
import org.neo4j.kernel.ha.com.master.SlaveFactory;

public class HighAvailabilitySlavesTest
{
    private static InstanceId instanceId1 = new InstanceId( 1 );
    private static URI clusterUri1 = create( "cluster://server1" );
    private static URI haUri1 = create( "ha://server1?serverId="+ instanceId1.toIntegerIndex() );
    private static InstanceId instanceId2 = new InstanceId( 2 );
    private static URI clusterUri2 = create( "cluster://server2" );
    private static URI haUri2 = create( "ha://server2?serverId="+ instanceId2.toIntegerIndex() );
    private static InstanceId instanceId3 = new InstanceId( 3 );
    private static URI clusterUri3 = create( "cluster://server3" );
    private static URI haUri3 = create( "ha://server3?serverId="+ instanceId3.toIntegerIndex() );

    @Test
    public void shouldRegisterItselfOnMonitors() throws Throwable
    {
        // given
        ClusterMembers clusterMembers = mock( ClusterMembers.class );
        Cluster cluster = mock( Cluster.class );
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        
        // when
        new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory).init();

        // then
        verify( cluster ).addClusterListener( Mockito.<ClusterListener>any() );
    }
    
    @Test
    public void shouldNotReturnUnavailableSlaves() throws Throwable
    {
        // given
        Cluster cluster = mock( Cluster.class);
        ClusterMembers clusterMembers = mock( ClusterMembers.class);
        when( clusterMembers.getMembers() ).thenReturn( Iterables.<ClusterMember,ClusterMember>iterable(
                new ClusterMember( instanceId1 ) ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory);
        slaves.init();

        // when
        Iterable<Slave> memberSlaves = slaves.getSlaves();

        // then
        Assert.assertThat( count( memberSlaves ), CoreMatchers.equalTo( 0L ));
    }

    @Test
    public void shouldNotReturnAvailableButFailedSlaves() throws Throwable
    {
        // given
        Cluster cluster = mock( Cluster.class);
        ClusterMembers clusterMembers = mock( ClusterMembers.class);
        when( clusterMembers.getMembers() ).thenReturn( Iterables.<ClusterMember,ClusterMember>iterable(
                new ClusterMember( instanceId1 ).availableAs( "SLAVE", haUri1 ).failed() ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory);
        slaves.init();

        // when
        Iterable<Slave> memberSlaves = slaves.getSlaves();

        // then
        Assert.assertThat( count( memberSlaves ), CoreMatchers.equalTo( 0L ));
    }

    @Test
    public void shouldReturnAvailableAndAliveSlaves() throws Throwable
    {
        // given
        Cluster cluster = mock( Cluster.class);
        ClusterMembers clusterMembers = mock( ClusterMembers.class);
        when( clusterMembers.getMembers() ).thenReturn( Iterables.<ClusterMember,ClusterMember>iterable(
                new ClusterMember( instanceId1 ).availableAs( HighAvailabilityModeSwitcher.SLAVE, haUri1) ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        when( slaveFactory.newSlave( (ClusterMember)any() ) ).thenReturn( mock(Slave.class));

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory);
        slaves.init();

        // when
        Iterable<Slave> memberSlaves = slaves.getSlaves();

        // then
        Assert.assertThat( count( memberSlaves ), CoreMatchers.equalTo( 1L ));
    }
    
    @Test
    public void shouldClearSlavesWhenNewMasterElected() throws Throwable
    {
        // given
        Cluster cluster = mock( Cluster.class );
        ClusterMembers clusterMembers = mock( ClusterMembers.class);
        when( clusterMembers.getMembers() ).thenReturn( Iterables.<ClusterMember,ClusterMember>iterable(
                new ClusterMember( instanceId1 ).availableAs( HighAvailabilityModeSwitcher.SLAVE, haUri1 ) ) );

        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        when( slaveFactory.newSlave( (ClusterMember)any() ) ).thenReturn( mock(Slave.class), mock(Slave.class) );

        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMembers, cluster, slaveFactory);
        slaves.init();

        ArgumentCaptor<ClusterListener> listener = ArgumentCaptor.forClass( ClusterListener.class );
        verify( cluster ).addClusterListener( listener.capture() );

        // when
        Slave slave1 = slaves.getSlaves().iterator().next();

        listener.getValue().elected( ClusterConfiguration.COORDINATOR, instanceId1, clusterUri2 );

        Slave slave2 = slaves.getSlaves().iterator().next();

        // then
        Assert.assertThat( slave2, not( sameInstance( slave1 ) ));
    }
}
