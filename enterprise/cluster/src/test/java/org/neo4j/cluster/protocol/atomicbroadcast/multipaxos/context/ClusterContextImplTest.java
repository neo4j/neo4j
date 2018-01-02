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
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.context;

import java.net.URI;
import java.util.concurrent.Executor;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectInputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectOutputStreamFactory;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.LearnerContext;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatContext;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.cluster.timeout.Timeouts;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
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
    public void electorLeavingTheClusterMustBeRemovedAsElector() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        ClusterConfiguration clusterConfiguration = mock(ClusterConfiguration.class);
        when( clusterConfiguration.getUriForId( elector ) ).thenReturn( URI.create("cluster://instance2") );

        CommonContextState commonContextState = mock( CommonContextState.class );
        when( commonContextState.configuration() ).thenReturn( clusterConfiguration );

        ClusterContext context = new ClusterContextImpl(me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock ( Executor.class ), mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), mock( HeartbeatContext.class ) );

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
    public void nonElectorLeavingTheClusterMustNotAffectElectorInformation() throws Throwable
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );
        InstanceId other = new InstanceId( 3 );

        ClusterConfiguration clusterConfiguration = mock(ClusterConfiguration.class);
        when( clusterConfiguration.getUriForId( other ) ).thenReturn( URI.create("cluster://instance2") );

        CommonContextState commonContextState = mock( CommonContextState.class );
        when( commonContextState.configuration() ).thenReturn( clusterConfiguration );

        ClusterContext context = new ClusterContextImpl(me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock ( Executor.class ), mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), mock( HeartbeatContext.class ) );

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
    public void instanceEnteringTheClusterMustBeRemovedAsElector() throws Exception
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );

        ClusterContext context = new ClusterContextImpl(me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock ( Executor.class ), mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), mock( HeartbeatContext.class ) );

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
    public void instanceEnteringTheClusterMustBeNotAffectElectorStatusIfItWasNotElectorBefore() throws Exception
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );
        InstanceId other = new InstanceId( 3 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );

        ClusterContext context = new ClusterContextImpl(me, commonContextState, NullLogProvider.getInstance(),
                mock( Timeouts.class ), mock ( Executor.class ), mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), mock( HeartbeatContext.class ) );

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
    public void electorFailingMustCauseElectorVersionToBeReset() throws Exception
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock ( HeartbeatContext.class );

        ArgumentCaptor<HeartbeatListener> listenerCaptor = ArgumentCaptor.forClass( HeartbeatListener.class );

        ClusterContext context = new ClusterContextImpl(me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), heartbeatContext );

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
    public void nonElectorFailingMustNotCauseElectorVersionToBeReset() throws Exception
    {
        // Given
        InstanceId me = new InstanceId( 1 );
        InstanceId elector = new InstanceId( 2 );

        CommonContextState commonContextState = mock( CommonContextState.class, RETURNS_MOCKS );
        Timeouts timeouts = mock( Timeouts.class );
        Executor executor = mock( Executor.class );

        HeartbeatContext heartbeatContext = mock ( HeartbeatContext.class );

        ArgumentCaptor<HeartbeatListener> listenerCaptor = ArgumentCaptor.forClass( HeartbeatListener.class );

        ClusterContext context = new ClusterContextImpl(me, commonContextState, NullLogProvider.getInstance(),
                timeouts, executor, mock( ObjectOutputStreamFactory.class ), mock(
                ObjectInputStreamFactory.class ), mock( LearnerContext.class ), heartbeatContext );

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
}
