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
package org.neo4j.causalclustering.core.consensus.log.debug;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategy;
import org.neo4j.causalclustering.core.consensus.log.segmented.CoreLogPruningStrategyFactory;
import org.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionFactory;
import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_reader_pool_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_rotation_size;
import static org.neo4j.causalclustering.core.consensus.log.RaftLogHelper.readLogEntry;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.logging.NullLogProvider.getInstance;

public class ReplayRaftLog
{
    private ReplayRaftLog()
    {
    }

    public static void main( String[] args ) throws IOException
    {
        Args arg = Args.parse( args );

        String from = arg.get( "from" );
        System.out.println("From is " + from);
        String to = arg.get( "to" );
        System.out.println("to is " + to);

        File logDirectory = new File( from );
        System.out.println( "logDirectory = " + logDirectory );
        Config config = Config.defaults( stringMap() );

        try ( DefaultFileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            LogProvider logProvider = getInstance();
            CoreLogPruningStrategy pruningStrategy =
                    new CoreLogPruningStrategyFactory( config.get( raft_log_pruning_strategy ), logProvider ).newInstance();
            SegmentedRaftLog log = new SegmentedRaftLog( fileSystem, logDirectory, config.get( raft_log_rotation_size ),
                    new CoreReplicatedContentMarshal(), logProvider, config.get( raft_log_reader_pool_size ),
                    Clocks.systemClock(), new OnDemandJobScheduler(), pruningStrategy );

            long totalCommittedEntries = log.appendIndex(); // Not really, but we need to have a way to pass in the commit index
            for ( int i = 0; i <= totalCommittedEntries; i++ )
            {
                ReplicatedContent content = readLogEntry( log, i ).content();
                if ( content instanceof ReplicatedTransaction )
                {
                    ReplicatedTransaction tx = (ReplicatedTransaction) content;
                    ReplicatedTransactionFactory.extractTransactionRepresentation( tx, new byte[0] ).accept( element ->
                    {
                        System.out.println( element );
                        return false;
                    } );
                }
            }
        }
    }
}
