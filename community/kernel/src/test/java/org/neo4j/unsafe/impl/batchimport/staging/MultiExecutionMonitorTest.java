/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MultiExecutionMonitorTest
{
    @Test
    public void shouldCheckMultipleMonitors() throws Exception
    {
        // GIVEN
        FakeClock clock = new FakeClock();
        TestableMonitor first = new TestableMonitor( clock, 100, MILLISECONDS );
        TestableMonitor other = new TestableMonitor( clock, 250, MILLISECONDS );
        MultiExecutionMonitor multiMonitor = new MultiExecutionMonitor( clock, first, other );
        StageExecution execution = mock( StageExecution.class );
        when( execution.stillExecuting() ).thenReturn( true );

        // WHEN
        OtherThreadExecutor<Void> t2 = cleanup.add( new OtherThreadExecutor<Void>( "T2", null ) );
        Future<Object> future = t2.executeDontWait( monitor( clock, multiMonitor, execution ) );
        clock.forward( 110, MILLISECONDS );
        awaitChecks( first, 1, other, 0 );
        clock.forward( 100, MILLISECONDS );
        awaitChecks( first, 2, other, 0 );
        clock.forward( 45, MILLISECONDS );
        awaitChecks( first, 2, other, 1 );

        // THEN
        when( execution.stillExecuting() ).thenReturn( false );
        future.get();
    }

    private void awaitChecks( Object... alternatingMonitorAndCount )
    {
        long endTime = currentTimeMillis()+SECONDS.toMillis( 2 );
        while ( currentTimeMillis() < endTime )
        {
            boolean match = true;
            for ( int i = 0; i < alternatingMonitorAndCount.length; i++ )
            {
                TestableMonitor monitor = (TestableMonitor) alternatingMonitorAndCount[i++];
                int count = (Integer) alternatingMonitorAndCount[i];
                assertThat( monitor.timesPolled, lessThanOrEqualTo( count ) );
                if ( monitor.timesPolled < count )
                {
                    match = false;
                    break;
                }
            }
            if ( match )
            {
                return;
            }
        }
        fail( "Polls didn't occur" );
    }

    private WorkerCommand<Void,Object> monitor( final Clock clock, final ExecutionMonitor multiMonitor,
            final StageExecution execution )
    {
        return new WorkerCommand<Void,Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                new ExecutionSupervisor( clock, multiMonitor ).supervise( execution );
                return null;
            }
        };
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();

    private static class TestableMonitor extends ExecutionMonitor.Adpter
    {
        private int timesPolled;

        public TestableMonitor( Clock clock, long interval, TimeUnit unit )
        {
            super( clock, interval, unit );
        }

        @Override
        public void check( StageExecution[] executions )
        {
            timesPolled++;
        }
    }
}
