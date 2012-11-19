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
package org.neo4j.kernel.ha.cluster.member;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.helpers.collection.IteratorUtil.asCollection;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.cluster.BindingListener;
import org.neo4j.cluster.ClusterMonitor;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.kernel.ha.SlaveFactory;
import org.neo4j.kernel.ha.cluster.HighAvailability;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.logging.DevNullLoggingService;

public class HighAvailabilitySlavesTest
{
    private static URI uri( String string )
    {
        try
        {
            return new URI( string );
        }
        catch ( URISyntaxException e )
        {
            throw new Error( e );
        }
    }
    
    private static URI clusterUri1 = uri( "cluster://server1" );
    private static URI haUri1 = uri( "ha://server1?serverId=1" );
    private static URI clusterUri2 = uri( "cluster://server2" );
    private static URI haUri2 = uri( "ha://server2?serverId=2" );
    private static URI clusterUri3 = uri( "cluster://server3" );
    private static URI haUri3 = uri( "ha://server3?serverId=3" );
    
    @Test
    public void shouldRegisterItselfOnMonitors() throws Exception
    {
        // given
        ClusterMonitor clusterMonitor = mock( ClusterMonitor.class );
        HighAvailability highAvailability = mock( HighAvailability.class );
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        
        // when
        new HighAvailabilitySlaves( clusterMonitor, highAvailability, slaveFactory, new DevNullLoggingService() );
        
        // then
        verify( clusterMonitor ).addBindingListener( Mockito.<BindingListener>any() );
        verify( clusterMonitor ).addHeartbeatListener( Mockito.<HeartbeatListener>any() );
        verify( highAvailability ).addHighAvailabilityMemberListener( Mockito.<HighAvailabilityMemberListener>any() );
    }
    
    @Test
    public void shouldCreateSlavesWhenMaster() throws Exception
    {
        // given
        URI clusterUri1 = new URI( "cluster://server1" );
        URI haUri1 = new URI( "ha://server1?serverId=1" );
        URI clusterUri2 = new URI( "cluster://server2" );
        URI haUri2 = new URI( "ha://server2?serverId=2" );
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMonitor, highAvailability,
                slaveFactory, new DevNullLoggingService() );
        clusterMonitor.listeningAt( clusterUri1 );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        
        // when
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );
        
        // then
        verify( slaveFactory ).newSlave( haUri2 );
        assertEquals( 1, asCollection( slaves.getSlaves() ).size() );
    }

    @Test
    public void shouldAvoidCreatingSlavesWhenNotMaster() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMonitor, highAvailability,
                slaveFactory, new DevNullLoggingService() );
        clusterMonitor.listeningAt( clusterUri1 );
        highAvailability.masterIsElectedAndAvailable( clusterUri2, haUri2 );
        
        // when
        highAvailability.slaveIsAvailable( clusterUri1, haUri1 );
        
        // then
        verifyZeroInteractions( slaveFactory );
        assertEquals( 0, asCollection( slaves.getSlaves() ).size() );
    }
    
    @Test
    public void shouldNotReturnUnavailableSlaves() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMonitor, highAvailability,
                slaveFactory, new DevNullLoggingService() );
        clusterMonitor.listeningAt( clusterUri1 );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );

        // when
        clusterMonitor.failed( clusterUri2 );

        // then
        verify( slaveFactory ).newSlave( haUri2 );
        assertEquals( 0, asCollection( slaves.getSlaves() ).size() );
    }

    @Test
    public void shouldReturnSlavesBecomingAvailableAgain() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMonitor, highAvailability,
                slaveFactory, new DevNullLoggingService() );
        clusterMonitor.listeningAt( clusterUri1 );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );

        // when
        clusterMonitor.failed( clusterUri2 );
        clusterMonitor.alive( clusterUri2 );

        // then
        verify( slaveFactory ).newSlave( haUri2 );
        assertEquals( 1, asCollection( slaves.getSlaves() ).size() );
    }
    
    @Test
    public void shouldClearSlavesWhenNewMasterElected() throws Exception
    {
        // given
        MockedClusterMonitor clusterMonitor = new MockedClusterMonitor();
        MockedHighAvailability highAvailability = new MockedHighAvailability();
        SlaveFactory slaveFactory = mock( SlaveFactory.class );
        HighAvailabilitySlaves slaves = new HighAvailabilitySlaves( clusterMonitor, highAvailability,
                slaveFactory, new DevNullLoggingService() );
        clusterMonitor.listeningAt( clusterUri1 );
        highAvailability.masterIsElectedAndAvailable( clusterUri1, haUri1 );
        highAvailability.slaveIsAvailable( clusterUri2, haUri2 );
        highAvailability.slaveIsAvailable( clusterUri3, haUri3 );

        // when
        highAvailability.masterIsElected( clusterUri2, haUri2 );

        // then
        assertEquals( 0, asCollection( slaves.getSlaves() ).size() );
    }
}
