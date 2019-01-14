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

import static org.neo4j.causalclustering.ReplicationModule.SESSION_TRACKER_NAME;
import static org.neo4j.causalclustering.core.server.CoreServerModule.LAST_FLUSHED_NAME;
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
     * @param args [0] = data directory
     * @throws IOException When IO exception occurs.
     */
    public static void main( String[] args ) throws IOException, ClusterStateException
    {
        if ( args.length != 1 )
        {
            System.out.println( "usage: DumpClusterState <data directory>" );
            System.exit( 1 );
        }

        File dataDirectory = new File( args[0] );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            DumpClusterState dumpTool = new DumpClusterState( fileSystem, dataDirectory, System.out );
            dumpTool.dump();
        }
    }

    DumpClusterState( FileSystemAbstraction fs, File dataDirectory, PrintStream out ) throws ClusterStateException
    {
        this.fs = fs;
        this.clusterStateDirectory = new ClusterStateDirectory( dataDirectory ).initialize( fs ).get();
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
