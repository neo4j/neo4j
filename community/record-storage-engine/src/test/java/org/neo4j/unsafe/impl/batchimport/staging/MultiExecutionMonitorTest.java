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
package org.neo4j.unsafe.impl.batchimport.staging;

import org.junit.Rule;
import org.junit.Test;

import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.test.rule.CleanupRule;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class MultiExecutionMonitorTest
{
    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    @Test
    public void shouldCheckMultipleMonitors()
    {
        // GIVEN
        FakeClock clock = Clocks.fakeClock();
        TestableMonitor first = new TestableMonitor( clock, 100, MILLISECONDS, "first" );
        TestableMonitor other = new TestableMonitor( clock, 250, MILLISECONDS, "other" );
        MultiExecutionMonitor multiMonitor = new MultiExecutionMonitor( clock, first, other );

        // WHEN/THEN
        clock.forward( 110, MILLISECONDS );
        expectCallsToCheck( multiMonitor, first, 1, other, 0 );
        clock.forward( 100, MILLISECONDS );
        expectCallsToCheck( multiMonitor, first, 2, other, 0 );
        clock.forward( 45, MILLISECONDS );
        expectCallsToCheck( multiMonitor, first, 2, other, 1 );
    }

    private void expectCallsToCheck( ExecutionMonitor multiMonitor, Object... alternatingMonitorAndCount )
    {
        multiMonitor.check( null ); // null, knowing that our monitors in this test doesn't use 'em
        for ( int i = 0; i < alternatingMonitorAndCount.length; i++ )
        {
            TestableMonitor monitor = (TestableMonitor) alternatingMonitorAndCount[i++];
            int count = (Integer) alternatingMonitorAndCount[i];
            assertThat( monitor.timesPolled, lessThanOrEqualTo( count ) );
            if ( monitor.timesPolled < count )
            {
                fail( "Polls didn't occur, expected " + Arrays.toString( alternatingMonitorAndCount ) );
            }
        }
    }

    private static class TestableMonitor extends ExecutionMonitor.Adapter
    {
        private int timesPolled;
        private final String name;

        TestableMonitor( Clock clock, long interval, TimeUnit unit, String name )
        {
            super( clock, interval, unit );
            this.name = name;
        }

        @Override
        public void check( StageExecution execution )
        {
            timesPolled++;
        }

        @Override
        public String toString()
        {
            return "[" + name + ":" + timesPolled + "]";
        }
    }
}
