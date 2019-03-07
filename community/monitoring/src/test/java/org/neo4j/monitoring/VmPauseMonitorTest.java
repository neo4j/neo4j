/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.monitoring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.function.Consumer;

import org.neo4j.monitoring.VmPauseMonitor.VmPauseInfo;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.monitoring.VmPauseMonitor.Monitor.EMPTY;

class VmPauseMonitorTest
{
    @SuppressWarnings( "unchecked" )
    private final Consumer<VmPauseInfo> listener = Mockito.mock( Consumer.class );
    private final JobHandle jobHandle = Mockito.mock( JobHandle.class );
    private final JobScheduler jobScheduler = Mockito.mock( JobScheduler.class );
    private final VmPauseMonitor monitor = Mockito.spy( new VmPauseMonitor( ofMillis( 1 ), ofMillis( 0 ), EMPTY, jobScheduler, listener ) );

    @BeforeEach
    void setUp()
    {
        Mockito.doReturn( jobHandle ).when( jobScheduler ).schedule( ArgumentMatchers.any( Group.class ), ArgumentMatchers.any( Runnable.class ) );
    }

    @Test
    void testCtorParametersValidation()
    {
        assertThrows( NullPointerException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( 1 ), null, jobScheduler, listener ) );
        assertThrows( NullPointerException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( 1 ), EMPTY, null, listener ) );
        assertThrows( NullPointerException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( 1 ), EMPTY, jobScheduler, null ) );
        assertThrows( IllegalArgumentException.class,
                () -> new VmPauseMonitor( ofSeconds( 0 ), ofSeconds( 1 ), EMPTY, jobScheduler, listener ) );
        assertThrows( IllegalArgumentException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( -1 ), EMPTY, jobScheduler, listener ) );
        assertThrows( IllegalArgumentException.class,
                () -> new VmPauseMonitor( ofSeconds( -1 ), ofSeconds( 1 ), EMPTY, jobScheduler, listener ) );
    }

    @Test
    void testStartAndStop()
    {
        monitor.start();
        monitor.stop();

        Mockito.verify( jobScheduler ).schedule( ArgumentMatchers.any( Group.class ), ArgumentMatchers.any( Runnable.class ) );
        Mockito.verify( jobHandle ).cancel( ArgumentMatchers.eq( true ) );
    }

    @Test
    void testRestart()
    {
        monitor.start();
        monitor.stop();
        monitor.start();

        Mockito.verify( jobScheduler, Mockito.times( 2 ) ).schedule( ArgumentMatchers.any( Group.class ), ArgumentMatchers.any( Runnable.class ) );
        Mockito.verify( jobHandle ).cancel( ArgumentMatchers.eq( true ) );
    }

    @Test
    void testFailStopWithoutStart()
    {
        assertThrows( IllegalStateException.class, monitor::stop );
    }

    @Test
    void testFailOnDoubleStart()
    {
        assertThrows( IllegalStateException.class, () ->
        {
            monitor.start();
            monitor.start();
        } );
    }

    @Test
    void testFailOnDoubleStop()
    {
        assertThrows( IllegalStateException.class, () ->
        {
            monitor.start();
            monitor.stop();
            monitor.stop();
        } );
    }

    @Test
    void testNotifyListener() throws Exception
    {
        Mockito.doReturn( false, true ).when( monitor ).isStopped();
        monitor.monitor();
        Mockito.verify( listener ).accept( ArgumentMatchers.any(VmPauseInfo.class) );
    }
}
