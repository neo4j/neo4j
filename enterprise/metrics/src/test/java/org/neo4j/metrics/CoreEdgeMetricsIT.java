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
package org.neo4j.metrics;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.metrics.source.causalclustering.CatchUpMetrics;
import org.neo4j.metrics.source.causalclustering.CoreMetrics;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_advertised_address;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.source.causalclustering.ReadReplicaMetrics.PULL_UPDATES;
import static org.neo4j.metrics.source.causalclustering.ReadReplicaMetrics.PULL_UPDATE_HIGHEST_TX_ID_RECEIVED;
import static org.neo4j.metrics.source.causalclustering.ReadReplicaMetrics.PULL_UPDATE_HIGHEST_TX_ID_REQUESTED;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CoreEdgeMetricsIT
{
    private static final int TIMEOUT = 15;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 1 )
            .withSharedCoreParam( MetricsSettings.metricsEnabled, Settings.TRUE )
            .withSharedReadReplicaParam( MetricsSettings.metricsEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvEnabled, Settings.TRUE )
            .withSharedReadReplicaParam( MetricsSettings.csvEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvInterval, "100ms" )
            .withSharedReadReplicaParam( MetricsSettings.csvInterval, "100ms" );

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
        cluster = clusterRule.startCluster();

        // when
        CoreClusterMember coreMember = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        for ( CoreClusterMember db : cluster.coreMembers() )
        {
            assertAllNodesVisible( db.database() );
        }

        for ( ReadReplica db : cluster.readReplicas() )
        {
            assertAllNodesVisible( db.database() );
        }

        File coreMetricsDir = new File( coreMember.homeDir(), csvPath.getDefaultValue() );

        assertEventually( "append index eventually accurate",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.APPEND_INDEX ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "commit index eventually accurate",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.COMMIT_INDEX ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "term eventually accurate",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.TERM ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "leader not found eventually accurate",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.LEADER_NOT_FOUND ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "tx pull requests received eventually accurate", () ->
        {
            long total = 0;
            for ( final File homeDir : cluster.coreMembers().stream().map( CoreClusterMember::homeDir ).collect( Collectors.toList()) )
            {
                File metricsDir = new File( homeDir, "metrics" );
                total += readLongValue( metricsCsv( metricsDir, CatchUpMetrics.TX_PULL_REQUESTS_RECEIVED ) );
            }
            return total;
        }, greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "tx retries eventually accurate",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.TX_RETRIES ) ), equalTo( 0L ),
                TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "is leader eventually accurate",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.IS_LEADER ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        File readReplicaMetricsDir = new File( cluster.getReadReplicaById( 0 ).homeDir(), "metrics" );

        assertEventually( "pull update request registered",
                () -> readLongValue( metricsCsv( readReplicaMetricsDir, PULL_UPDATES ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "pull update request registered",
                () -> readLongValue( metricsCsv( readReplicaMetricsDir, PULL_UPDATE_HIGHEST_TX_ID_REQUESTED ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "pull update response received",
                () -> readLongValue( metricsCsv( readReplicaMetricsDir, PULL_UPDATE_HIGHEST_TX_ID_RECEIVED ) ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );
    }

    private void assertAllNodesVisible( GraphDatabaseAPI db ) throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            ThrowingSupplier<Long, Exception> nodeCount = () -> count( db.getAllNodes() );

            Config config = db.getDependencyResolver().resolveDependency( Config.class );

            assertEventually( "node to appear on core server " + config.get( raft_advertised_address ), nodeCount,
                    greaterThan( 0L ), TIMEOUT, SECONDS );

            for ( Node node : db.getAllNodes() )
            {
                assertEquals( "baz_bat", node.getProperty( "foobar" ) );
            }

            tx.success();
        }
    }
}
