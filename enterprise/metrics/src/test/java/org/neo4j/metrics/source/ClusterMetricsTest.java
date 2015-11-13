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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.ha.cluster.modeswitch.HighAvailabilityModeSwitcher;
import org.neo4j.kernel.ha.cluster.member.ClusterMember;
import org.neo4j.kernel.ha.cluster.member.ClusterMembers;
import org.neo4j.kernel.ha.cluster.member.ObservedClusterMembers;
import org.neo4j.kernel.impl.logging.LogService;
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
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void clusterMetricsReportMasterAvailable()
    {
        // given
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.neoClusterEnabled.name(), Settings.TRUE ) );
        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        ClusterMembers clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.MASTER, HighAvailabilityMemberState.MASTER );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( dependencyResolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );
        LogService logService = mock( LogService.class );

        life.add( new ClusterMetrics( config, monitors, metricRegistry, dependencyResolver, logService ) );
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
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.neoClusterEnabled.name(), Settings.TRUE ) );
        ClusterMembers clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.SLAVE, HighAvailabilityMemberState.SLAVE );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( dependencyResolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );
        LogService logService = mock( LogService.class );

        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        life.add( new ClusterMetrics( config, monitors, metricRegistry, dependencyResolver, logService ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        //then wait for the reporter to get a report
        reporter.report();
        assertEquals( 0, reporter.isMasterValue );
        assertEquals( 1, reporter.isAvailableValue );
    }

    @Test
    public void testClusterMemberNotEnabled()
    {
        // given
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.neoClusterEnabled.name(), Settings.FALSE ) );
        ClusterMembers clusterMembers =
                getClusterMembers( HighAvailabilityModeSwitcher.SLAVE, HighAvailabilityMemberState.SLAVE );
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( dependencyResolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );
        LogService logService = mock( LogService.class );

        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        life.add( new ClusterMetrics( config, monitors, metricRegistry, dependencyResolver, logService ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        //then the reporter should fail
        thrown.expect( NullPointerException.class );
        reporter.report();
    }

    @Test
    public void testClusterMembersNull()
    {
        // given
        MetricRegistry metricRegistry = new MetricRegistry();
        Config config = new Config( stringMap( MetricsSettings.neoClusterEnabled.name(), Settings.TRUE ) );
        ClusterMembers clusterMembers = null;
        DependencyResolver dependencyResolver = mock( DependencyResolver.class );
        when( dependencyResolver.resolveDependency( ClusterMembers.class ) ).thenReturn( clusterMembers );
        LogService logService = mock( LogService.class );

        Monitors monitors = new Monitors();
        LifeSupport life = new LifeSupport();
        life.add( new ClusterMetrics( config, monitors, metricRegistry, dependencyResolver, logService ) );
        life.start();

        // when
        TestReporter reporter = new TestReporter( metricRegistry );
        reporter.start( 10, TimeUnit.MILLISECONDS );

        //then the reporter should fail
        reporter.report();
        assertEquals( 0, reporter.isMasterValue );
        assertEquals( 0, reporter.isAvailableValue );
    }

    ClusterMembers getClusterMembers( String memberRole, HighAvailabilityMemberState memberState )
    {
        HighAvailabilityMemberStateMachine stateMachine = mock( HighAvailabilityMemberStateMachine.class );
        when( stateMachine.getCurrentState() ).thenReturn( memberState );
        ClusterMember clusterMember = spy( new ClusterMember( new InstanceId( 1 ) ) );
        when( clusterMember.getHARole() ).thenReturn( memberRole );
        ObservedClusterMembers observedClusterMembers = mock( ObservedClusterMembers.class );
        when( observedClusterMembers.getCurrentMember() ).thenReturn( clusterMember );
        return new ClusterMembers( observedClusterMembers, stateMachine );
    }

    class TestReporter extends ScheduledReporter
    {
        private int isMasterValue, isAvailableValue;

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