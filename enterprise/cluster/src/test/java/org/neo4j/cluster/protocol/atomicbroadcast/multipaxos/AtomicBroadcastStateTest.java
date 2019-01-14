/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.protocol.atomicbroadcast.multipaxos;

import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.logging.NullLog;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.cluster.com.message.Message.to;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastMessage.failed;
import static org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.AtomicBroadcastState.broadcasting;

public class AtomicBroadcastStateTest
{
    @Test
    public void shouldNotBroadcastWhenHavingNoQuorumNoCoordinator() throws Throwable
    {
        // GIVEN
        AtomicBroadcastContext context = mock( AtomicBroadcastContext.class );
        when( context.hasQuorum() ).thenReturn( false );

        InstanceId coordinator = id( 1 );
        when( context.getCoordinator() ).thenReturn( coordinator );
        when( context.getUriForId( coordinator ) ).thenReturn( uri( 1 ) );
        when( context.getLog( AtomicBroadcastState.class ) ).thenReturn( NullLog.getInstance() );

        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder outgoing = messages::add;

        // WHEN
        broadcasting.handle( context, message( 1 ), outgoing );
        // THEN
        assertEquals( 0, messages.size() );
    }

    @Test
    public void shouldNotBroadcastWhenHavingNoQuorumButCoordinator() throws Throwable
    {
        // GIVEN
        AtomicBroadcastContext context = mock( AtomicBroadcastContext.class );
        when( context.hasQuorum() ).thenReturn( false );
        when( context.getCoordinator() ).thenReturn( null );
        when( context.getLog( AtomicBroadcastState.class ) ).thenReturn( NullLog.getInstance() );

        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder outgoing = messages::add;

        // WHEN
        broadcasting.handle( context, message( 1 ), outgoing );
        // THEN
        assertEquals( 0, messages.size() );
    }

    @Test
    public void shouldBroadcastWhenHavingQuorumAndCoordinator() throws Throwable
    {
        // GIVEN
        AtomicBroadcastContext context = mock( AtomicBroadcastContext.class );
        when( context.hasQuorum() ).thenReturn( true );

        InstanceId coordinator = id( 1 );
        when( context.getCoordinator() ).thenReturn( coordinator );
        when( context.getUriForId( coordinator ) ).thenReturn( uri( 1 ) );

        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder outgoing = messages::add;

        // WHEN
        broadcasting.handle( context, message( 1 ), outgoing );
        // THEN
        assertEquals( 1, messages.size() );

    }

    @Test
    public void shouldBroadcastWhenHavingQuorumNoCoordinator() throws Throwable
    {
        // GIVEN
        AtomicBroadcastContext context = mock( AtomicBroadcastContext.class );
        when( context.hasQuorum() ).thenReturn( true );
        when( context.getCoordinator() ).thenReturn( null );
        final List<Message<?>> messages = new ArrayList<>( 1 );
        MessageHolder outgoing = messages::add;
        // WHEN
        broadcasting.handle( context, message( 1 ), outgoing );
        // THEN
        assertEquals( 1, messages.size() );
    }

    private Message<AtomicBroadcastMessage> message( int id )
    {
        return to( failed, uri( id ), "some payload" ).setHeader( Message.HEADER_CONVERSATION_ID, "some id" );
    }

    private URI uri( int i )
    {
        return URI.create( "http://localhost:" + (6000 + i) + "?serverId=" + i );
    }

    private InstanceId id( int i )
    {
        return new InstanceId( i );
    }
}
