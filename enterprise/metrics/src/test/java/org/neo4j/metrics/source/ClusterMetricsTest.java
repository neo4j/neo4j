/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.metrics.source;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.sun.jdi.InvocationException;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.MapUtil.stringMap;


public class ClusterMetricsTest
{
    @Test
    public void clusterMetricsReportMasterAvailable() throws Exception
    {
        // given
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.clusterEnabled.name(), Settings.TRUE ) );
        ClusterMembers clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.MASTER, HighAvailabilityMemberState.MASTER );
        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        life.add( new ClusterMetrics( config, monitors, metricRegistry, clusterMembers ) );
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
    public void clusterMetricsReportSlaveAvailable() throws Exception
    {
        // given
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.clusterEnabled.name(), Settings.TRUE ) );
        ClusterMembers clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.SLAVE, HighAvailabilityMemberState.SLAVE );

        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        life.add( new ClusterMetrics( config, monitors, metricRegistry, clusterMembers ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        //then wait for the reporter to get a report
        reporter.report();
        assertEquals( 0, reporter.isMasterValue );
        assertEquals( 1, reporter.isAvailableValue );
    }

    ClusterMembers getClusterMembers( String memberRole, HighAvailabilityMemberState memberState )
    {
        HighAvailabilityMemberStateMachine stateMachine = mock( HighAvailabilityMemberStateMachine.class );
        when( stateMachine.getCurrentState() ).thenReturn( memberState );
        ClusterMember clusterMember = spy( new ClusterMember( new InstanceId( 1 ) ) );
        when( clusterMember.getHARole() ).thenReturn( memberRole );
        ObservedClusterMembers observedClusterMembers = mock( ObservedClusterMembers.class );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( clusterMember );
        ClusterMembers clusterMembers = new ClusterMembers( observedClusterMembers, stateMachine );
        return clusterMembers;
    }

    @Test
    public void testClusterMemberNotEnabled() throws Exception
    {
        // given
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.clusterEnabled.name(), Settings.FALSE ) );
        ClusterMembers clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.SLAVE, HighAvailabilityMemberState.SLAVE );

        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        life.add( new ClusterMetrics( config, monitors, metricRegistry, clusterMembers ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        //then the reporter should fail
        try
        {
            reporter.report();
            fail("Reporter should have failed since corresponding metrics are not enabled.");
        }
        catch(Exception e){
            //This should have thrown an exception
        }

    }


    class TestReporter extends ScheduledReporter
    {
        private int isMasterValue, isAvailableValue;
        private volatile boolean reported;

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
            reported = true;
        }
    }


}