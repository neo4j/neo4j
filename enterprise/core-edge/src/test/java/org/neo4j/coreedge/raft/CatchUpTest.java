/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft;

import java.util.LinkedList;

import org.junit.Test;

import org.neo4j.coreedge.raft.RaftMessages.NewEntry.Request;
import org.neo4j.coreedge.raft.log.RaftStorageException;
import org.neo4j.coreedge.raft.log.ReadableRaftLog;
import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.server.RaftTestMember;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.raft.ReplicatedInteger.valueOf;

public class CatchUpTest
{
    @Test
    public void happyClusterPropagatesUpdates() throws Throwable
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long[] allMembers = {leader, 1, 2};

        final RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( allMembers ) );
        final RaftTestMember leaderMember = fixture.members().withId( leader ).member();

        // when
        fixture.members().withId( leader ).timeoutService().invokeTimeout( RaftInstance.Timeouts.ELECTION );
        net.processMessages();
        fixture.members().withId( leader ).raftInstance().handle( new Request<>( leaderMember, valueOf( 42 ) ) );
        net.processMessages();

        // then
        for ( long aMember : allMembers )
        {
            assertLogEntries( fixture.members().withId( aMember ).raftLog(), 42 );
        }
    }

    @Test
    public void newMemberWithNoLogShouldCatchUpFromPeers() throws Throwable
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final long leaderId = 0;
        final long sleepyId = 2;

        final long[] awakeMembers = {leaderId, 1};
        final long[] allMembers = {leaderId, 1, sleepyId};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.members().withId( leaderId ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( allMembers ) );

        fixture.members().withId( leaderId ).timeoutService().invokeTimeout( RaftInstance.Timeouts.ELECTION );
        net.processMessages();

        final RaftTestMember leader = fixture.members().withId( leaderId ).member();

        net.disconnect( sleepyId );

        // when
        fixture.members().withId( leaderId ).raftInstance().handle( new Request<>( leader, valueOf( 10 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request<>( leader, valueOf( 20 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request<>( leader, valueOf( 30 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request<>( leader, valueOf( 40 ) ) );
        net.processMessages();

        // then
        for ( long awakeMember : awakeMembers )
        {
            assertLogEntries( fixture.members().withId( awakeMember ).raftLog(), 10, 20, 30, 40 );
        }

        int[] nothing = {};
        assertLogEntries( fixture.members().withId( sleepyId ).raftLog(), nothing );

        // when
        net.reconnect( sleepyId );
        Thread.sleep( 500 ); // TODO: This needs an injectable/controllable timeout service for the log shipper.
        net.processMessages();

        // then
        assertLogEntries( fixture.members().withId( sleepyId ).raftLog(), 10, 20, 30, 40 );
    }

    private void assertLogEntries( ReadableRaftLog log, int... expectedValues ) throws RaftStorageException
    {
        LinkedList<Integer> expected = new LinkedList<>();
        for ( int value : expectedValues )
        {
            expected.add( value );
        }

        for ( long logIndex = 0; logIndex <= log.appendIndex(); logIndex++ )
        {
            ReplicatedContent content = log.readEntryContent( logIndex );
            if ( content instanceof ReplicatedInteger )
            {
                ReplicatedInteger integer = (ReplicatedInteger) content;
                assertEquals( (int) expected.pop(), integer.get() );
            }
        }
        assertTrue( expected.isEmpty() );
    }
}
