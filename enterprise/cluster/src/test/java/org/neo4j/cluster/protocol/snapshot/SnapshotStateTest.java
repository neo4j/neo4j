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
package org.neo4j.cluster.protocol.snapshot;

import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnapshotStateTest
{
    @Test
    public void testNoSnapshotRequestIfCoordinatorInExistingCluster() throws Throwable
    {
        Map<InstanceId, URI> extraMember = new HashMap<>();
        URI other = URI.create( "cluster://other");
        extraMember.put( new InstanceId( 2 ), other );
        baseNoSendTest( extraMember );
    }

    @Test
    public void testNoSnapshotRequestIfOnlyMember() throws Throwable
    {
        Map<InstanceId, URI> extraMember = new HashMap<>();
        baseNoSendTest( extraMember );
    }

    private void baseNoSendTest( Map<InstanceId,URI> extraMembers ) throws Throwable
    {
        URI me = URI.create( "cluster://me" );

        Map<InstanceId,URI> members = new HashMap<>();
        final InstanceId myId = new InstanceId( 1 );
        members.put( myId, me );
        members.putAll( extraMembers );

        ClusterConfiguration clusterConfiguration = mock( ClusterConfiguration.class );
        when( clusterConfiguration.getMembers() ).thenReturn( members );
        when( clusterConfiguration.getElected( ClusterConfiguration.COORDINATOR ) ).thenReturn( myId );
        when( clusterConfiguration.getUriForId( myId ) ).thenReturn( me );

        ClusterContext clusterContext = mock( ClusterContext.class );
        when( clusterContext.getConfiguration() ).thenReturn( clusterConfiguration );
        when( clusterContext.getMyId() ).thenReturn( myId );

        SnapshotContext context = mock( SnapshotContext.class );
        when( context.getClusterContext() ).thenReturn( clusterContext );
        SnapshotProvider snapshotProvider = mock( SnapshotProvider.class );
        when( context.getSnapshotProvider() ).thenReturn( snapshotProvider );

        Message<SnapshotMessage> message = Message.to( SnapshotMessage.refreshSnapshot, me );

        MessageHolder outgoing = mock( MessageHolder.class );

        SnapshotState newState = (SnapshotState) SnapshotState.ready.handle( context, message, outgoing );
        assertThat( newState, equalTo( SnapshotState.ready ) );
        Mockito.verifyZeroInteractions( outgoing );
    }
}
