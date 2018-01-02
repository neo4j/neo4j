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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.causalclustering.scenarios.SampleData.createData;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CorePruningIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.state_machine_flush_window_size, "1" )
                    .withSharedCoreParam( raft_log_pruning_strategy, "keep_none" )
                    .withSharedCoreParam( CausalClusteringSettings.raft_log_rotation_size, "1K" )
                    .withSharedCoreParam( CausalClusteringSettings.raft_log_pruning_frequency, "100ms" );

    @Test
    public void actuallyDeletesTheFiles() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember coreGraphDatabase = null;
        int txs = 10;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) ->
            {
                createData( db, 1 );
                tx.success();
            } );
        }

        // when pruning kicks in then some files are actually deleted
        File raftLogDir = coreGraphDatabase.raftLogDirectory();
        int expectedNumberOfLogFilesAfterPruning = 2;
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( raftLogDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 5, TimeUnit.SECONDS );
    }

    @Test
    public void shouldNotPruneUncommittedEntries() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember coreGraphDatabase = null;
        int txs = 1000;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) -> createData( db, 1 ) );
        }

        // when pruning kicks in then some files are actually deleted
        int expectedNumberOfLogFilesAfterPruning = 2;
        File raftLogDir = coreGraphDatabase.raftLogDirectory();
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( raftLogDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 5, TimeUnit.SECONDS );
    }

    private int numberOfFiles( File raftLogDir ) throws RuntimeException
    {
        return raftLogDir.list().length;
    }
}
