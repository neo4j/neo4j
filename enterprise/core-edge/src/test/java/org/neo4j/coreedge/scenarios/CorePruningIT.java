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
package org.neo4j.coreedge.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule;
import org.neo4j.test.coreedge.ClusterRule;

import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;
import static org.neo4j.coreedge.scenarios.CoreToCoreCopySnapshotIT.createData;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CorePruningIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreServers( 3 )
            .withNumberOfEdgeServers( 0 )
            .withSharedCoreParam( CoreEdgeClusterSettings.state_machine_flush_window_size, "1" )
            .withSharedCoreParam( CoreEdgeClusterSettings.raft_log_pruning, "1 entries" )
            .withSharedCoreParam( CoreEdgeClusterSettings.raft_log_rotation_size, "1K" )
            .withSharedCoreParam( CoreEdgeClusterSettings.raft_log_pruning_frequency, "100ms" );

    @Test
    public void actuallyDeletesTheFiles() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreGraphDatabase coreGraphDatabase = null;
        int txs = 10;
        for ( int i = 0; i < txs; i++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) -> {
                createData( db, 1 );
                tx.success();
            } );
        }

        // when pruning kicks in then some files are actually deleted
        File storeDir = new File( coreGraphDatabase.getStoreDir() );
        int expectedNumberOfLogFilesAfterPruning = 2;
        assertEventually( "raft logs eventually pruned", () -> numberOfFiles( storeDir ),
                equalTo( expectedNumberOfLogFilesAfterPruning ), 1, TimeUnit.SECONDS );
    }

    private int numberOfFiles( File storeDir ) throws RuntimeException
    {
        File clusterDir = new File( storeDir, EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME );
        File raftLogDir = new File( clusterDir, SEGMENTED_LOG_DIRECTORY_NAME );
        return raftLogDir.list().length;
    }
}
