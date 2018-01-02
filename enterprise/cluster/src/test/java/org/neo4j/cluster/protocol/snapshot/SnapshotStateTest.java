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
package org.neo4j.cluster.protocol.snapshot;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.mockito.Mockito;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

public class SnapshotStateTest
{
    @Test
    public void testNoSnapshotRequestIfCoordinatorInExistingCluster() throws Throwable
    {
        Map<InstanceId, URI> extraMember = new HashMap<InstanceId, URI>();
        URI other = URI.create( "cluster://other");
        extraMember.put( new InstanceId( 2 ), other );
        baseNoSendTest( extraMember );
    }

    @Test
    public void testNoSnapshotRequestIfOnlyMember() throws Throwable
    {
        Map<InstanceId, URI> extraMember = new HashMap<InstanceId, URI>();
        baseNoSendTest( extraMember );
    }

    public void baseNoSendTest( Map<InstanceId, URI> extraMembers) throws Throwable
    {
        URI me = URI.create( "cluster://me" );

        Map<InstanceId, URI> members = new HashMap<InstanceId, URI>();
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
        when( context.getSnapshotProvider() ).thenReturn( mock( SnapshotProvider.class ) );

        Message<SnapshotMessage> message = Message.to( SnapshotMessage.refreshSnapshot, me );

        MessageHolder outgoing = mock( MessageHolder.class );

        SnapshotState newState = (SnapshotState) SnapshotState.ready.handle( context, message, outgoing );
        assertThat( newState, equalTo( SnapshotState.ready ) );
        Mockito.verifyZeroInteractions( outgoing );
    }
}
