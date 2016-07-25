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
package org.neo4j.coreedge.raft.log.debug;

import java.io.File;
import java.io.IOException;
import java.time.Clock;

import org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog;
import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;
import org.neo4j.coreedge.raft.replication.ReplicatedContent;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransaction;
import org.neo4j.coreedge.raft.replication.tx.ReplicatedTransactionFactory;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.coreedge.raft.log.RaftLogHelper.readLogEntry;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ReplayRaftLog
{
    public static void main( String[] args ) throws IOException
    {
        Args arg = Args.parse( args );

        String from = arg.get( "from" );
        System.out.println("From is " + from);
        String to = arg.get( "to" );
        System.out.println("to is " + to);

        File logDirectory = new File( from );
        System.out.println( "logDirectory = " + logDirectory );
        Config config = new Config( stringMap() );

        SegmentedRaftLog log = new SegmentedRaftLog( new DefaultFileSystemAbstraction(), logDirectory,
                config.get( CoreEdgeClusterSettings.raft_log_rotation_size ), new CoreReplicatedContentMarshal(),
                NullLogProvider.getInstance(), config.get( CoreEdgeClusterSettings.raft_log_pruning_strategy ),
                config.get( CoreEdgeClusterSettings.raft_log_reader_pool_size ), Clock.systemUTC() );

        long totalCommittedEntries = log.appendIndex(); // Not really, but we need to have a way to pass in the commit index
        for ( int i = 0; i <= totalCommittedEntries; i++ )
        {
            ReplicatedContent content = readLogEntry( log, i ).content();
            if ( content instanceof ReplicatedTransaction )
            {
                ReplicatedTransaction tx = (ReplicatedTransaction) content;
                ReplicatedTransactionFactory.extractTransactionRepresentation( tx, new byte[0] ).accept(
                        element -> {
                            System.out.println(element);
                            return false;
                        } );
            }
        }
    }
}
