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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.edge.EdgeGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.metrics.source.CoreMetrics;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.coreedge.server.CoreEdgeClusterSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.Assert.assertEventually;


import org.neo4j.coreedge.discovery.Cluster;

public class CoreEdgeMetricsIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldMonitorCoreEdge() throws Exception
    {
        // given
        File dbDir = dir.directory();
        Cluster cluster = Cluster.start( dbDir, 3, 1 );

        // when
        GraphDatabaseService coreDB = cluster.findLeader( 5000 );

        try ( Transaction tx = coreDB.beginTx() )
        {
            Node node = coreDB.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        }

        // then
        for ( final CoreGraphDatabase db : cluster.coreServers() )
        {
            assertAllNodesVisible(db);
        }

        for ( final EdgeGraphDatabase db : cluster.edgeServers() )
        {
            assertAllNodesVisible( db );
        }

        File appendMetrics = metricsCsv( dbDir, CoreMetrics.APPEND_INDEX );
        assertThat( readLastValue( appendMetrics ), greaterThan( 0L ) );

        File commitMetrics = metricsCsv( dbDir, CoreMetrics.COMMIT_INDEX );
        assertThat( readLastValue( commitMetrics ), greaterThan( 0L ) );

        File termMetrics = metricsCsv( dbDir, CoreMetrics.TERM );
        assertThat( readLastValue( termMetrics ), greaterThan( 0L ) );

        File leaderNotFoundMetrics = metricsCsv( dbDir, CoreMetrics.LEADER_NOT_FOUND );
        assertThat( readLastValue( leaderNotFoundMetrics ), equalTo( 0L ) );

        File txPullRequestsMetrics = metricsCsv( dbDir, CoreMetrics.TX_PULL_REQUESTS_RECEIVED );
        assertThat( readLastValue( txPullRequestsMetrics ), greaterThan( 0L ) );

        File txRetryMetrics = metricsCsv( dbDir, CoreMetrics.TX_RETRIES );
        assertThat( readLastValue( txRetryMetrics ), equalTo( 0L ) );

        File isLeaderMetrics = metricsCsv( dbDir, CoreMetrics.IS_LEADER );
        assertThat( readLastValue( isLeaderMetrics ), greaterThanOrEqualTo ( 0L ) );

        cluster.shutdown();
    }

    private void assertAllNodesVisible( GraphDatabaseFacade db ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

            Config config = db.getDependencyResolver().resolveDependency( Config.class );

            assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                    greaterThan(  0L ), 15, SECONDS );

            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                assertEquals( "baz_bat", node.getProperty( "foobar" ) );
            }

            tx.success();
        }
    }

    private File metricsCsv( File dbDir, String metric )
    {
        File csvFile = new File( dbDir, "/server-core-0/metrics/" + metric + ".csv" );
        assertEventually( "Metrics file should exist", csvFile::exists, is( true ), 20, SECONDS );
        return csvFile;
    }

    private static final int TIME_STAMP = 0;
    private static final int METRICS_VALUE = 1;


    private long readLastValue( File metricFile ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( metricFile ) ) )
        {
            String[] headers = reader.readLine().split( "," );
            assertThat( headers.length, is( 2 ) );
            assertThat( headers[TIME_STAMP], is( "t" ) );
            assertThat( headers[METRICS_VALUE], is( "value" ) );

            String line = reader.readLine();
            String[] fields = line.split( "," );
            return Long.valueOf( fields[METRICS_VALUE] );
        }
    }

}
