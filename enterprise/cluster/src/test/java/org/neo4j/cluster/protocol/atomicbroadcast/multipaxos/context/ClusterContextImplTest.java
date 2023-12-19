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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.net.URI;
import java.util.concurrent.Executor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.cluster.ClusterMessage;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterContextImplTest
{
    /*
     * This test ensures that an instance that cleanly leaves the cluster is no longer assumed to be an elector. This
     * has the effect that when it rejoins its elector version will be reset and its results will go through
     */
    @Test
    public void electorLeavingTheClusterMustBeRemovedAsElector()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        ClusterConfiguration clusterConfiguration = mock(ClusterConfiguration.class);
        when( clusterConfiguration.getUriForId( elector ) ).thenReturn( URI.create("cluster://instance2") );

        CommonContextState commonContextState = mock( CommonContextState.class );
        when( commonContextState.configuration() ).thenReturn( clusterConfiguration );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock( Executor.class ), mock( ObjectOutputStreamFactory.class ),
                mock( ObjectInputStreamFactory.class ), mock( LearnerContext.class ),
                mock( HeartbeatContext.class ), mock( Config.class ) );

          // This means instance 2 was the elector at version 8
        context.setLastElector( elector );
        context.setLastElectorVersion( 8 );

        // When
        context.left( elector );

        // Then
        assertEquals( context.getLastElector(), InstanceId.NONE );
        assertEquals( context.getLastElectorVersion(), -1 );
    }

    /*
     * This test ensures that an instance that cleanly leaves the cluster but is not the elector has no effect on
     * elector id and last version
     */
    @Test
    public void nonElectorLeavingTheClusterMustNotAffectElectorInformation()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );
        InstanceId other = new InstanceId( 3 );

        ClusterConfiguration clusterConfiguration = mock(ClusterConfiguration.class);
        when( clusterConfiguration.getUriForId( other ) ).thenReturn( URI.create("cluster://instance2") );

        CommonContextState commonContextState = mock( CommonContextState.class );
        when( commonContextState.configuration() ).thenReturn( clusterConfiguration );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock( Executor.class ), mock( ObjectOutputStreamFactory.class ),
                mock( ObjectInputStreamFactory.class ), mock( LearnerContext.class ),
                mock( HeartbeatContext.class ),mock( Config.class ) );

          // This means instance 2 was the elector at version 8
        context.setLastElector( elector );
        context.setLastElectorVersion( 8 );

        // When
        context.left( other );

        // Then
        assertEquals( context.getLastElector(), elector );
        assertEquals( context.getLastElectorVersion(), 8 );
    }

    /*
     * This test ensures that an instance that enters the cluster has its elector version reset. That means that
     * if it was the elector before its version is now reset so results can be applied. This and the previous tests
     * actually perform the same things at different events, one covering for the other.
     */
    @Test
    public void instanceEnteringTheClusterMustBeRemovedAsElector()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock( Executor.class ), mock( ObjectOutputStreamFactory.class ),
                mock( ObjectInputStreamFactory.class ), mock( LearnerContext.class ),
                mock( HeartbeatContext.class ), mock( Config.class ) );

        // This means instance 2 was the elector at version 8
        context.setLastElector( elector );
        context.setLastElectorVersion( 8 );

        // When
        context.joined( elector, URI.create( "cluster://elector" ) );

        // Then
        assertEquals( context.getLastElector(), InstanceId.NONE );
        assertEquals( context.getLastElectorVersion(), -1 );
    }

    /*
     * This test ensures that a joining instance that was not marked as elector before does not affect the
     * current elector version. This is the complement of the previous test.
     */
    @Test
    public void instanceEnteringTheClusterMustBeNotAffectElectorStatusIfItWasNotElectorBefore()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );
        InstanceId other = new InstanceId( 3 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock( Executor.class ), mock( ObjectOutputStreamFactory.class ),
                mock( ObjectInputStreamFactory.class ), mock( LearnerContext.class ),
                mock( HeartbeatContext.class ), mock( Config.class )  );

        // This means instance 2 was the elector at version 8
        context.setLastElector( elector );
        context.setLastElectorVersion( 8 );

        // When
        context.joined( other, URI.create( "cluster://other" ) );

        // Then
        assertEquals( context.getLastElector(), elector );
        assertEquals( context.getLastElectorVersion(), 8 );
    }

    /*
     * This test ensures that an instance that is marked as failed has its elector version reset. This means that
     * the instance, once it comes back, will still be able to do elections even if it lost state
     */
    @Test
    public void electorFailingMustCauseElectorVersionToBeReset()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );

        ArgumentCaptor<HeartbeatListener> listenerCaptor = ArgumentCaptor.forClass( HeartbeatListener.class );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock( ObjectInputStreamFactory.class ),
                mock( LearnerContext.class ), heartbeatContext, mock( Config.class ) );

        verify( heartbeatContext ).addHeartbeatListener( listenerCaptor.capture() );

        HeartbeatListener theListener = listenerCaptor.getValue();

        // This means instance 2 was the elector at version 8
        context.setLastElector( elector );
        context.setLastElectorVersion( 8 );

        // When
        theListener.failed( elector );

        // Then
        assertEquals( context.getLastElector(), InstanceId.NONE );
        assertEquals( context.getLastElectorVersion(), ClusterContextImpl.NO_ELECTOR_VERSION );
    }

    /*
     * This test ensures that an instance that is marked as failed has its elector version reset. This means that
     * the instance, once it comes back, will still be able to do elections even if it lost state
     */
    @Test
    public void nonElectorFailingMustNotCauseElectorVersionToBeReset()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );

        ArgumentCaptor<HeartbeatListener> listenerCaptor = ArgumentCaptor.forClass( HeartbeatListener.class );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock( ObjectInputStreamFactory.class ),
                mock( LearnerContext.class ), heartbeatContext, mock( Config.class ) );

        verify( heartbeatContext ).addHeartbeatListener( listenerCaptor.capture() );

        HeartbeatListener theListener = listenerCaptor.getValue();

        // This means instance 2 was the elector at version 8
        context.setLastElector( elector );
        context.setLastElectorVersion( 8 );

        // When
        theListener.failed( new InstanceId( 3 ) );

        // Then
        assertEquals( context.getLastElector(), elector );
        assertEquals( context.getLastElectorVersion(), 8 );
    }

    @Test
    public void shouldGracefullyHandleEmptyDiscoveryHeader()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId joining = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), heartbeatContext, mock( Config.class ) );

        ClusterMessage.ConfigurationRequestState request = mock( ClusterMessage.ConfigurationRequestState.class );
        when( request.getJoiningId() ).thenReturn( joining );

        // When
        // Instance 2 contacts us with a request but it is empty
        context.addContactingInstance( request, "" );

        // Then
        // The discovery header we generate should still contain that instance
        assertEquals( "2", context.generateDiscoveryHeader() );
    }

    @Test
    public void shouldUpdateDiscoveryHeaderWithContactingInstances()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId joiningOne = new InstanceId( 2 );
        InstanceId joiningTwo = new InstanceId( 3 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), heartbeatContext, mock( Config.class ) );

        ClusterMessage.ConfigurationRequestState requestOne = mock( ClusterMessage.ConfigurationRequestState.class );
        when( requestOne.getJoiningId() ).thenReturn( joiningOne );

        ClusterMessage.ConfigurationRequestState requestTwo = mock( ClusterMessage.ConfigurationRequestState.class );
        when( requestTwo.getJoiningId() ).thenReturn( joiningTwo );

        // When
        // Instance 2 contacts us twice and Instance 3 contacts us once
        context.addContactingInstance( requestOne, "4, 5" ); // discovery headers are random here
        context.addContactingInstance( requestOne, "4, 5" );
        context.addContactingInstance( requestTwo, "2, 5" );

        // Then
        // The discovery header we generate should still contain one copy of each instance
        assertEquals( "2,3", context.generateDiscoveryHeader() );
    }

    @Test
    public void shouldKeepTrackOfInstancesWeHaveContacted()
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId joiningOne = new InstanceId( 2 );
        InstanceId joiningTwo = new InstanceId( 3 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock( HeartbeatContext.class );

        ClusterContext context = new ClusterContextImpl( me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), heartbeatContext, mock( Config.class ) );

        ClusterMessage.ConfigurationRequestState requestOne = mock( ClusterMessage.ConfigurationRequestState.class );
        when( requestOne.getJoiningId() ).thenReturn( joiningOne );

        ClusterMessage.ConfigurationRequestState requestTwo = mock( ClusterMessage.ConfigurationRequestState.class );
        when( requestTwo.getJoiningId() ).thenReturn( joiningTwo );

        // When
        // Instance two contacts us but we are not in the header
        context.addContactingInstance( requestOne, "4, 5" );
        // Then we haven't contacted instance 2
        assertFalse(context.haveWeContactedInstance( requestOne ) );

        // When
        // Instance 2 reports that we have contacted it after all
        context.addContactingInstance( requestOne, "4, 5, 1" );
        // Then
        assertTrue(context.haveWeContactedInstance( requestOne ) );

        // When
        // Instance 3 says we have contacted it
        context.addContactingInstance( requestTwo, "2, 5, 1" );
        // Then
        assertTrue( context.haveWeContactedInstance( requestTwo ) );

        // When
        // For some reason we are not in the header of 3 in subsequent responses (a delayed one, for example)
        context.addContactingInstance( requestTwo, "2, 5" );
        // Then
        // The state should still keep the fact we've contacted it already
        assertTrue( context.haveWeContactedInstance( requestTwo ) );
    }
}
