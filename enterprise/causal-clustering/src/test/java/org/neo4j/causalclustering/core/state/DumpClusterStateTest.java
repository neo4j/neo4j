/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.core.state.storage.SimpleStorage;
import org.neo4j.causalclustering.core.state.storage.StateMarshal;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.server.CoreServerModule.LAST_FLUSHED_NAME;
import static org.neo4j.causalclustering.ReplicationModule.SESSION_TRACKER_NAME;
import static org.neo4j.causalclustering.core.IdentityModule.CORE_MEMBER_ID_NAME;
import static org.neo4j.causalclustering.core.consensus.ConsensusModule.RAFT_MEMBERSHIP_NAME;
import static org.neo4j.causalclustering.core.consensus.ConsensusModule.RAFT_TERM_NAME;
import static org.neo4j.causalclustering.core.consensus.ConsensusModule.RAFT_VOTE_NAME;
import static org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule.ID_ALLOCATION_NAME;
import static org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule.LOCK_TOKEN_NAME;

public class DumpClusterStateTest
{
    @Rule
    public EphemeralFileSystemRule fsa = new EphemeralFileSystemRule();
    private File clusterStateDirectory = new File( "cluster-state" );

    @Test
    public void shouldDumpClusterState() throws Exception
    {
        // given
        createStates();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DumpClusterState dumpTool = new DumpClusterState( fsa.get(), clusterStateDirectory, new PrintStream( out ) );

        // when
        dumpTool.dump();

        // then
        int lineCount = out.toString().split( System.lineSeparator() ).length;
        assertEquals( 8, lineCount );
    }

    private void createStates() throws IOException
    {
        SimpleStorage<MemberId> memberIdStorage = new SimpleFileStorage<>( fsa.get(), clusterStateDirectory, CORE_MEMBER_ID_NAME, new MemberId.Marshal(), NullLogProvider.getInstance() );
        memberIdStorage.writeState( new MemberId( UUID.randomUUID() ) );

        createDurableState( LAST_FLUSHED_NAME, new LongIndexMarshal() );
        createDurableState( LOCK_TOKEN_NAME, new ReplicatedLockTokenState.Marshal( new MemberId.Marshal() ) );
        createDurableState( ID_ALLOCATION_NAME, new IdAllocationState.Marshal() );
        createDurableState( SESSION_TRACKER_NAME, new GlobalSessionTrackerState.Marshal( new MemberId.Marshal() ) );

        /* raft state */
        createDurableState( RAFT_MEMBERSHIP_NAME, new RaftMembershipState.Marshal() );
        createDurableState( RAFT_TERM_NAME, new TermState.Marshal() );
        createDurableState( RAFT_VOTE_NAME, new VoteState.Marshal( new MemberId.Marshal() ) );
    }

    private <T> void createDurableState( String name, StateMarshal<T> marshal ) throws IOException
    {
        DurableStateStorage<T> storage = new DurableStateStorage<>(
                fsa.get(), clusterStateDirectory, name, marshal, 1024, NullLogProvider.getInstance() );

        //noinspection EmptyTryBlock: Will create initial state.
        try ( Lifespan ignored = new Lifespan( storage ) )
        {
        }
    }
}
