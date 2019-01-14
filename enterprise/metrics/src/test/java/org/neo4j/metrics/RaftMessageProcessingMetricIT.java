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
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.metrics.source.causalclustering.CoreMetrics;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.neo4j.metrics.MetricsSettings.csvPath;
import static org.neo4j.metrics.MetricsTestHelper.metricsCsv;
import static org.neo4j.metrics.MetricsTestHelper.readLongValue;
import static org.neo4j.metrics.MetricsTestHelper.readTimerDoubleValue;
import static org.neo4j.metrics.MetricsTestHelper.readTimerLongValueAndAssert;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class RaftMessageProcessingMetricIT
{
    private static final int TIMEOUT = 15;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 0 )
            .withSharedCoreParam( CausalClusteringSettings.leader_election_timeout, "1s" )
            .withSharedCoreParam( MetricsSettings.metricsEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvEnabled, Settings.TRUE )
            .withSharedCoreParam( MetricsSettings.csvInterval, "100ms" );

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
    public void shouldMonitorMessageDelay() throws Throwable
    {
        // given
        cluster = clusterRule.startCluster();

        // then
        CoreClusterMember leader = cluster.awaitLeader();
        File coreMetricsDir = new File( leader.homeDir(), csvPath.getDefaultValue() );

        assertEventually( "message delay eventually recorded",
                () -> readLongValue( metricsCsv( coreMetricsDir, CoreMetrics.DELAY ) ),
                greaterThanOrEqualTo( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "message timer count eventually recorded",
                () -> readTimerLongValueAndAssert( metricsCsv( coreMetricsDir, CoreMetrics.TIMER ), ( newValue, currentValue ) -> newValue >= currentValue,
                        MetricsTestHelper.TimerField.COUNT ),
                greaterThan( 0L ), TIMEOUT, TimeUnit.SECONDS );

        assertEventually( "message timer max eventually recorded",
                () -> readTimerDoubleValue( metricsCsv( coreMetricsDir, CoreMetrics.TIMER ), MetricsTestHelper.TimerField.MAX ),
                greaterThanOrEqualTo( 0d ), TIMEOUT, TimeUnit.SECONDS );
    }
}
