/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.monitoring;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Consumer;

import org.neo4j.kernel.monitoring.VmPauseMonitor.VmPauseInfo;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobScheduler;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class VmPauseMonitorTest
{
    @SuppressWarnings( "unchecked" )
    private final Consumer<VmPauseInfo> listener = mock( Consumer.class );
    private final JobHandle jobHandle = mock( JobHandle.class );
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final VmPauseMonitor monitor = spy( new VmPauseMonitor( ofMillis( 1 ), ofMillis( 0 ), NullLog.getInstance(), jobScheduler, listener ) );

    @BeforeEach
    void setUp()
    {
        doReturn( jobHandle ).when( jobScheduler ).schedule( any( Group.class ), any( Runnable.class ) );
    }

    @Test
    void testCtorParametersValidation()
    {
        assertThrows( NullPointerException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( 1 ), null, jobScheduler, listener ) );
        assertThrows( NullPointerException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( 1 ), NullLog.getInstance(), null, listener ) );
        assertThrows( NullPointerException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( 1 ), NullLog.getInstance(), jobScheduler, null ) );
        assertThrows( IllegalArgumentException.class,
                () -> new VmPauseMonitor( ofSeconds( 0 ), ofSeconds( 1 ), NullLog.getInstance(), jobScheduler, listener ) );
        assertThrows( IllegalArgumentException.class,
                () -> new VmPauseMonitor( ofSeconds( 1 ), ofSeconds( -1 ), NullLog.getInstance(), jobScheduler, listener ) );
        assertThrows( IllegalArgumentException.class,
                () -> new VmPauseMonitor( ofSeconds( -1 ), ofSeconds( 1 ), NullLog.getInstance(), jobScheduler, listener ) );
    }

    @Test
    void testStartAndStop()
    {
        monitor.start();
        monitor.stop();

        verify( jobScheduler ).schedule( any( Group.class ), any( Runnable.class ) );
        verify( jobHandle ).cancel( eq( true ) );
    }

    @Test
    void testRestart()
    {
        monitor.start();
        monitor.stop();
        monitor.start();

        verify( jobScheduler, times( 2 ) ).schedule( any( Group.class ), any( Runnable.class ) );
        verify( jobHandle ).cancel( eq( true ) );
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
        doReturn( false, true ).when( monitor ).isStopped();
        monitor.monitor();
        verify( listener ).accept( any(VmPauseInfo.class) );
    }
}
