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
package org.neo4j.metrics;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.edge.EdgeGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.metrics.source.coreedge.CoreMetrics;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.source.coreedge.EdgeMetrics.PULL_UPDATES;
import static org.neo4j.metrics.source.coreedge.EdgeMetrics.PULL_UPDATE_HIGHEST_TX_ID_RECEIVED;
import static org.neo4j.metrics.source.coreedge.EdgeMetrics.PULL_UPDATE_HIGHEST_TX_ID_REQUESTED;
import static org.neo4j.test.Assert.assertEventually;

public class CoreEdgeMetricsIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldMonitorCoreEdge() throws Exception
    {
        // given
        File dbDir = dir.directory();
        cluster = Cluster.start( dbDir, 3, 1 );

        // when
        GraphDatabaseService coreDB = cluster.awaitLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            assertAllNodesVisible( db );
        }

        for ( final EdgeGraphDatabase db : cluster.edgeServers() )
        {
            assertAllNodesVisible( db );
        }

        File coreServerMetricsDir = new File( cluster.getCoreServerById( 0 ).getStoreDir(), "metrics" );

        assertEventually( "append index eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.APPEND_INDEX ) ),
                greaterThan( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "commit index eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.COMMIT_INDEX ) ),
                greaterThan( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "term eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.TERM ) ),
                greaterThanOrEqualTo( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "leader not found eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.LEADER_NOT_FOUND ) ),
                equalTo( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "tx pull requests received eventually accurate",
                () ->
                {
                    long total = 0;
                    for ( final CoreGraphDatabase db : cluster.coreServers() )
                    {
                        File metricsDir = new File( db.getStoreDir(), "metrics" );
                        total += readLongValue( metricsCsv( metricsDir, CoreMetrics.TX_PULL_REQUESTS_RECEIVED ) );
                    }
                    return total;
                },
                greaterThan( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "tx retries eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.TX_RETRIES ) ),
                equalTo( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "is leader eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.IS_LEADER ) ),
                greaterThanOrEqualTo( 0L ), 5, TimeUnit.SECONDS );

        File edgeServerMetricsDir = new File( cluster.getEdgeServerById( 0 ).getStoreDir(), "metrics" );

        assertEventually( "pull update request registered",
                () -> readLongValue( metricsCsv( edgeServerMetricsDir, PULL_UPDATES ) ),
                greaterThan( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "pull update request registered",
                () ->
                        readLongValue( metricsCsv( edgeServerMetricsDir, PULL_UPDATE_HIGHEST_TX_ID_REQUESTED ) ),
                greaterThan( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "pull update response received",
                () ->
                        readLongValue( metricsCsv( edgeServerMetricsDir, PULL_UPDATE_HIGHEST_TX_ID_RECEIVED ) ),
                greaterThan( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "dropped messages eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.DROPPED_MESSAGES ) ),
                greaterThanOrEqualTo( 0L ), 5, TimeUnit.SECONDS );

        assertEventually( "queue size eventually accurate",
                () -> readLongValue( metricsCsv( coreServerMetricsDir, CoreMetrics.QUEUE_SIZE ) ),
                greaterThanOrEqualTo( 0L ), 5, TimeUnit.SECONDS );
    }

    private void assertAllNodesVisible( GraphDatabaseFacade db ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

            Config config = db.getDependencyResolver().resolveDependency( Config.class );

            assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                    greaterThan( 0L ), 15, SECONDS );

            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                assertEquals( "baz_bat", node.getProperty( "foobar" ) );
            }

            tx.success();
        }
    }
}
