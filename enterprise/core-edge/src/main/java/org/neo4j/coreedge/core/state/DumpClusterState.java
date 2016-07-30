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
package org.neo4j.coreedge.core.state;

import java.io.File;
import java.io.IOException;

import org.neo4j.coreedge.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.coreedge.core.state.id.IdAllocationState;
import org.neo4j.coreedge.raft.membership.RaftMembershipState;
import org.neo4j.coreedge.raft.term.TermState;
import org.neo4j.coreedge.raft.vote.VoteState;
import org.neo4j.coreedge.identity.MemberId.MemberIdMarshal;
import org.neo4j.coreedge.core.state.locks.ReplicatedLockTokenState;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.coreedge.core.state.CoreStateMachinesModule.ID_ALLOCATION_NAME;
import static org.neo4j.coreedge.core.state.CoreStateMachinesModule.LOCK_TOKEN_NAME;
import static org.neo4j.coreedge.ReplicationModule.LAST_FLUSHED_NAME;
import static org.neo4j.coreedge.ReplicationModule.SESSION_TRACKER_NAME;
import static org.neo4j.coreedge.raft.ConsensusModule.RAFT_MEMBERSHIP_NAME;
import static org.neo4j.coreedge.raft.ConsensusModule.RAFT_TERM_NAME;
import static org.neo4j.coreedge.raft.ConsensusModule.RAFT_VOTE_NAME;
import static org.neo4j.coreedge.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.coreedge.core.EnterpriseCoreEditionModule.CORE_MEMBER_ID_NAME;

public class DumpClusterState
{
    public static void main( String[] args ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File baseDir = new File( args[0], CLUSTER_STATE_DIRECTORY_NAME );

        /* core state */
        dumpState( fs, baseDir, LAST_FLUSHED_NAME, new LongIndexMarshal() );
        dumpState( fs, baseDir, CORE_MEMBER_ID_NAME, new MemberIdMarshal() );

        dumpState( fs, baseDir, LOCK_TOKEN_NAME, new ReplicatedLockTokenState.Marshal( new MemberIdMarshal() ) );
        dumpState( fs, baseDir, ID_ALLOCATION_NAME, new IdAllocationState.Marshal() );
        dumpState( fs, baseDir, SESSION_TRACKER_NAME, new GlobalSessionTrackerState.Marshal( new MemberIdMarshal() ) );

        /* raft state */
        dumpState( fs, baseDir, RAFT_MEMBERSHIP_NAME, new RaftMembershipState.Marshal() );
        dumpState( fs, baseDir, RAFT_TERM_NAME, new TermState.Marshal() );
        dumpState( fs, baseDir, RAFT_VOTE_NAME, new VoteState.Marshal( new MemberIdMarshal() ) );
    }

    private static void dumpState( FileSystemAbstraction fs, File baseDir, String name, StateMarshal<?> marshal ) throws IOException
    {
        DurableStateStorage<?> storage = new DurableStateStorage<>(
                fs, baseDir, name, marshal, 1024, null, NullLogProvider.getInstance() );

        System.out.println( name + ": " + storage.getInitialState() );
        storage.shutdown();
    }
}
