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

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.coreedge.catchup.storecopy.StoreFiles;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.core.consensus.roles.Role;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;
import org.neo4j.time.Clocks;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.raft_log_pruning_frequency;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.raft_log_pruning_strategy;
import static org.neo4j.coreedge.core.CoreEdgeClusterSettings.raft_log_rotation_size;
import static org.neo4j.coreedge.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.coreedge.scenarios.SampleData.createData;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CoreToCoreCopySnapshotIT
{
    private static final int TIMEOUT_MS = 5000;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 );

    @Test
    public void shouldBeAbleToDownloadLargerFreshSnapshot() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        CoreClusterMember source = cluster.coreTx( ( db, tx ) -> {
            createData( db, 1000 );
            tx.success();
        } );

        // when
        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( TIMEOUT_MS, Role.FOLLOWER );

        // shutdown the follower, remove the store, restart
        follower.shutdown();
        new StoreFiles( new DefaultFileSystemAbstraction() ).delete( follower.storeDir() );
        follower.start();

        // then
        assertEquals( DbRepresentation.of( source.database() ), DbRepresentation.of( follower.database() ) );
    }

    @Test
    public void shouldBeAbleToDownloadToNewInstanceAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CoreEdgeClusterSettings.state_machine_flush_window_size.name(), "1",
                CoreEdgeClusterSettings.raft_log_pruning_strategy.name(), "3 entries",
                CoreEdgeClusterSettings.raft_log_rotation_size.name(), "1K" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) -> {
            createData( db, 10000 );
            tx.success();
        } );

        // when
        for ( CoreClusterMember coreDb : cluster.coreMembers() )
        {
            coreDb.coreState().prune();
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
        int numberOfTransactions = 100;
        Timeout timeout = new Timeout( Clocks.systemClock(), 60, SECONDS );

        // start the cluster
        Cluster cluster = clusterRule.withSharedCoreParams( coreParams ).startCluster();

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
        dataMatchesEventually( firstServer, cluster.coreMembers() );
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

    private int getOldestLogIdOn( CoreClusterMember clusterMember ) throws TimeoutException
    {
        return clusterMember.getLogFileNames().firstKey().intValue();
    }

    private int getMostRecentLogIdOn( CoreClusterMember clusterMember ) throws TimeoutException
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
