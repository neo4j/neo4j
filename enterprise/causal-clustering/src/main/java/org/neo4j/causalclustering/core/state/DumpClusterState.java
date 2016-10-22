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

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
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
import org.neo4j.causalclustering.identity.MemberId.Marshal;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.causalclustering.core.server.CoreServerModule.LAST_FLUSHED_NAME;
import static org.neo4j.causalclustering.ReplicationModule.SESSION_TRACKER_NAME;
import static org.neo4j.causalclustering.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.causalclustering.core.IdentityModule.CORE_MEMBER_ID_NAME;
import static org.neo4j.causalclustering.core.consensus.ConsensusModule.RAFT_MEMBERSHIP_NAME;
import static org.neo4j.causalclustering.core.consensus.ConsensusModule.RAFT_TERM_NAME;
import static org.neo4j.causalclustering.core.consensus.ConsensusModule.RAFT_VOTE_NAME;
import static org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule.ID_ALLOCATION_NAME;
import static org.neo4j.causalclustering.core.state.machines.CoreStateMachinesModule.LOCK_TOKEN_NAME;

public class DumpClusterState
{
    private final FileSystemAbstraction fs;
    private final File clusterStateDirectory;
    private final PrintStream out;

    /**
     * @param args [0] = graph database folder
     * @throws IOException When IO exception occurs.
     */
    public static void main( String[] args ) throws IOException
    {
        if ( args.length != 1 )
        {
            System.out.println( "usage: DumpClusterState <graph.db>" );
            System.exit( 1 );
        }

        DumpClusterState dumpTool = new DumpClusterState(
                new DefaultFileSystemAbstraction(),
                new File( args[0], CLUSTER_STATE_DIRECTORY_NAME ),
                System.out );

        dumpTool.dump();
    }

    DumpClusterState( FileSystemAbstraction fs, File clusterStateDirectory, PrintStream out )
    {
        this.fs = fs;
        this.clusterStateDirectory = clusterStateDirectory;
        this.out = out;
    }

    void dump() throws IOException
    {
        SimpleStorage<MemberId> memberIdStorage = new SimpleFileStorage<>( fs, clusterStateDirectory, CORE_MEMBER_ID_NAME,
                new Marshal(), NullLogProvider.getInstance() );
        if ( memberIdStorage.exists() )
        {
            MemberId memberId = memberIdStorage.readState();
            out.println( CORE_MEMBER_ID_NAME + ": " + memberId );
        }

        dumpState( LAST_FLUSHED_NAME, new LongIndexMarshal() );
        dumpState( LOCK_TOKEN_NAME, new ReplicatedLockTokenState.Marshal( new Marshal() ) );
        dumpState( ID_ALLOCATION_NAME, new IdAllocationState.Marshal() );
        dumpState( SESSION_TRACKER_NAME, new GlobalSessionTrackerState.Marshal( new Marshal() ) );

        /* raft state */
        dumpState( RAFT_MEMBERSHIP_NAME, new RaftMembershipState.Marshal() );
        dumpState( RAFT_TERM_NAME, new TermState.Marshal() );
        dumpState( RAFT_VOTE_NAME, new VoteState.Marshal( new Marshal() ) );
    }

    private void dumpState( String name, StateMarshal<?> marshal )
    {
        int rotationSize = Config.defaults().get( CausalClusteringSettings.replicated_lock_token_state_size );
        DurableStateStorage<?> storage =
                new DurableStateStorage<>( fs, clusterStateDirectory, name, marshal, rotationSize,
                        NullLogProvider.getInstance() );

        if ( storage.exists() )
        {
            try ( Lifespan ignored = new Lifespan( storage ) )
            {
                out.println( name + ": " + storage.getInitialState() );
            }
        }
    }
}
