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
package org.neo4j.metrics.source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.neo4j.cluster.InstanceId;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.source.cluster.ClusterMetrics;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class ClusterMetricsTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private final MetricRegistry metricRegistry = new MetricRegistry();
    private final Monitors monitors = new Monitors();
    private final LifeSupport life = new LifeSupport();

    @Test
    public void clusterMetricsReportMasterAvailable()
    {
        // given
        Supplier<ClusterMembers> clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.MASTER, HighAvailabilityMemberState.MASTER );

        life.add( new ClusterMetrics( monitors, metricRegistry, clusterMembers ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        // then wait for the reporter to get a report
        reporter.report();
        assertEquals( 1, reporter.isMasterValue );
        assertEquals( 1, reporter.isAvailableValue );
    }

    @Test
    public void clusterMetricsReportSlaveAvailable()
    {
        // given
        Supplier<ClusterMembers> clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.SLAVE, HighAvailabilityMemberState.SLAVE );

        life.add( new ClusterMetrics( monitors, metricRegistry, clusterMembers ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        //then wait for the reporter to get a report
        reporter.report();
        assertEquals( 0, reporter.isMasterValue );
        assertEquals( 1, reporter.isAvailableValue );
    }

    private static Supplier<ClusterMembers> getClusterMembers( String memberRole, HighAvailabilityMemberState memberState )
    {
        HighAvailabilityMemberStateMachine stateMachine = mock( HighAvailabilityMemberStateMachine.class );
        when( stateMachine.getCurrentState() ).thenReturn( memberState );
        ClusterMember clusterMember = spy( new ClusterMember( new InstanceId( 1 ) ) );
        when( clusterMember.getHARole() ).thenReturn( memberRole );
        ObservedClusterMembers observedClusterMembers = mock( ObservedClusterMembers.class );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( clusterMember );
        return () -> new ClusterMembers( observedClusterMembers, stateMachine );
    }

    private class TestReporter extends ScheduledReporter
    {
        private int isMasterValue;
        private int isAvailableValue;

        protected TestReporter( MetricRegistry registry )
        {
            super( registry, "TestReporter", MetricFilter.ALL, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS );
        }

        @Override
        public void report( SortedMap<String,Gauge> gauges, SortedMap<String,Counter> counters,
                SortedMap<String,Histogram> histograms, SortedMap<String,Meter> meters, SortedMap<String,Timer> timers )
        {
            isMasterValue = (Integer) gauges.get( ClusterMetrics.IS_MASTER ).getValue();
            isAvailableValue = (Integer) gauges.get( ClusterMetrics.IS_AVAILABLE ).getValue();
        }
    }

}
