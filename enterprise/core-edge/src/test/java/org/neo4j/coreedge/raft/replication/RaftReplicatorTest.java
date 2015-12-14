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
package org.neo4j.coreedge.raft.replication;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import org.neo4j.coreedge.raft.DirectNetworking;
import org.neo4j.coreedge.raft.RaftInstance;
import org.neo4j.coreedge.raft.RaftTestFixture;
import org.neo4j.coreedge.raft.ReplicatedString;
import org.neo4j.coreedge.raft.membership.RaftTestGroup;
import org.neo4j.coreedge.server.RaftTestMember;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.server.RaftTestMember.member;

public class RaftReplicatorTest
{
    private static final ReplicatedString CONTENT = new ReplicatedString( "la la la" );

    @Test
    public void shouldYieldEntryContentOnceLogsAreSafelyReplicated() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        // given
        final long leader = 0;
        final long[] allMembers = {leader, 1, 2};

        final RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.members().withId( leader ).raftInstance().bootstrapWithInitialMembers( new RaftTestGroup( allMembers ) );
        fixture.members().withId( leader ).timeoutService().invokeTimeout( RaftInstance.Timeouts.ELECTION );
        net.processMessages();

        RaftReplicator<RaftTestMember> replicator =
                new RaftReplicator<>( fixture.members().withId( leader ).raftInstance(),
                        member( 0 ), net.new Outbound( 0 ) );

        fixture.members().withId( leader ).raftLog().registerListener( replicator );

        ReplicatedContentListener listener = new ReplicatedContentListener();
        replicator.subscribe( listener );

        // when
        replicator.replicate( CONTENT );
        net.processMessages();

        // then
        assertEquals( CONTENT, listener.getContent().get( 3, SECONDS ) );
    }

    private class ReplicatedContentListener implements Replicator.ReplicatedContentListener
    {
        private CompletableFuture<ReplicatedContent> content = new CompletableFuture<>();

        @Override
        public void onReplicated( ReplicatedContent value, long logIndex )
        {
            this.content.complete( value );
        }

        public Future<ReplicatedContent> getContent()
        {
            return content;
        }
    }
}
