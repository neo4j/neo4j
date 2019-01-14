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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_frequency;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_pruning_strategy;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_log_rotation_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.state_machine_flush_window_size;
import static org.neo4j.causalclustering.discovery.Cluster.dataOnMemberEventuallyLooksLike;
import static org.neo4j.causalclustering.scenarios.SampleData.createData;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Note that this test is extended in the blockdevice repository.
 */
public class CoreToCoreCopySnapshotIT
{
    protected static final int NR_CORE_MEMBERS = 3;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( NR_CORE_MEMBERS )
            .withNumberOfReadReplicas( 0 );

    @Test
    public void shouldBeAbleToDownloadLargerFreshSnapshot() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember source = cluster.coreTx( ( db, tx ) ->
        {
            createData( db, 1000 );
            tx.success();
        } );

        // when
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 5, TimeUnit.SECONDS );

        // shutdown the follower, remove the store, restart
        follower.shutdown();
        deleteDirectoryRecursively( follower.storeDir(), follower.serverId() );
        deleteDirectoryRecursively( follower.clusterStateDirectory(), follower.serverId() );
        follower.start();

        // then
        assertEquals( DbRepresentation.of( source.database() ), DbRepresentation.of( follower.database() ) );
    }

    protected void deleteDirectoryRecursively( File directory, int id ) throws IOException
    {
        // Extracted so the inheriting test in the block device repository can override it. id is used there.
        FileUtils.deleteRecursively( directory );
    }

    @Test
    public void shouldBeAbleToDownloadToNewInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CausalClusteringSettings.state_machine_flush_window_size.name(), "1",
                CausalClusteringSettings.raft_log_pruning_strategy.name(), "3 entries",
                CausalClusteringSettings.raft_log_rotation_size.name(), "1K" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            createData( db, 10000 );
            tx.success();
        } );

        // when
        for ( CoreClusterMember coreDb : cluster.coreMembers() )
        {
            coreDb.raftLogPruner().prune();
        }

        cluster.removeCoreMember( leader ); // to force a change of leader
        leader = cluster.awaitLeader();

        int newDbId = 3;
        cluster.addCoreMemberWithId( newDbId ).start();
        CoreGraphDatabase newDb = cluster.getCoreMemberById( newDbId ).database();

        // then
        assertEquals( DbRepresentation.of( leader.database() ), DbRepresentation.of( newDb ) );
    }

    @Test
    public void shouldBeAbleToDownloadToRejoinedInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> coreParams = stringMap();
        coreParams.put( raft_log_rotation_size.name(), "1K" );
        coreParams.put( raft_log_pruning_strategy.name(), "keep_none" );
        coreParams.put( raft_log_pruning_frequency.name(), "100ms" );
        coreParams.put( state_machine_flush_window_size.name(), "64" );
        int numberOfTransactions = 100;

        // start the cluster
        Cluster cluster = clusterRule.withSharedCoreParams( coreParams ).startCluster();
        Timeout timeout = new Timeout( Clocks.systemClock(), 120, SECONDS );

        // accumulate some log files
        int firstServerLogFileCount;
        CoreClusterMember firstServer;
        do
        {
            timeout.assertNotTimedOut();
            firstServer = doSomeTransactions( cluster, numberOfTransactions );
            firstServerLogFileCount = getMostRecentLogIdOn( firstServer );
        }
        while ( firstServerLogFileCount < 5 );
        firstServer.shutdown();

        /* After shutdown we wait until we accumulate enough logs, and so that enough of the old ones
         * have been pruned, so that the rejoined instance won't be able to catch up to without a snapshot. */
        int oldestLogOnSecondServer;
        CoreClusterMember secondServer;
        do
        {
            timeout.assertNotTimedOut();
            secondServer = doSomeTransactions( cluster, numberOfTransactions );
            oldestLogOnSecondServer = getOldestLogIdOn( secondServer );
        }
        while ( oldestLogOnSecondServer < firstServerLogFileCount + 5 );

        // when
        firstServer.start();

        // then
        dataOnMemberEventuallyLooksLike( firstServer, secondServer );
    }

    private class Timeout
    {
        private final Clock clock;
        private final long absoluteTimeoutMillis;

        Timeout( Clock clock, long time, TimeUnit unit )
        {
            this.clock = clock;
            this.absoluteTimeoutMillis = clock.millis() + unit.toMillis( time );
        }

        void assertNotTimedOut()
        {
            if ( clock.millis() > absoluteTimeoutMillis )
            {
                throw new AssertionError( "Timed out" );
            }
        }
    }

    private int getOldestLogIdOn( CoreClusterMember clusterMember ) throws IOException
    {
        return clusterMember.getLogFileNames().firstKey().intValue();
    }

    private int getMostRecentLogIdOn( CoreClusterMember clusterMember ) throws IOException
    {
        return clusterMember.getLogFileNames().lastKey().intValue();
    }

    private CoreClusterMember doSomeTransactions( Cluster cluster, int count )
    {
        try
        {
            CoreClusterMember last = null;
            for ( int i = 0; i < count; i++ )
            {
                last = cluster.coreTx( ( db, tx ) ->
                {
                    Node node = db.createNode();
                    node.setProperty( "that's a bam", string( 1024 ) );
                    tx.success();
                } );
            }
            return last;
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    private String string( int numberOfCharacters )
    {
        StringBuilder s = new StringBuilder();
        for ( int i = 0; i < numberOfCharacters; i++ )
        {
            s.append( String.valueOf( i ) );
        }
        return s.toString();
    }

}
