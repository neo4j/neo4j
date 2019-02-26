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
package org.neo4j.kernel.impl.api.index;

import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.time.FakeClock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.kernel.impl.api.index.LoggingPhaseTracker.PERIOD_INTERVAL;

class LoggingPhaseTrackerTest
{
    private FakeClock clock = new FakeClock();

    @Test
    void shouldLogSingleTime()
    {
        LoggingPhaseTracker phaseTracker = getPhaseTracker();

        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.stop();

        EnumMap<PhaseTracker.Phase,LoggingPhaseTracker.Logger> times = phaseTracker.times();
        for ( Map.Entry<PhaseTracker.Phase,LoggingPhaseTracker.Logger> phaseEntry : times.entrySet() )
        {
            LoggingPhaseTracker.Logger logger = phaseEntry.getValue();
            if ( phaseEntry.getKey() == PhaseTracker.Phase.SCAN )
            {
                assertTrue( logger.totalTime >= 100 );
                assertTrue( logger.totalTime < 500 );
            }
            else
            {
                assertEquals( 0, logger.totalTime );
            }
        }
    }

    @Test
    void shouldLogMultipleTimes()
    {
        LoggingPhaseTracker phaseTracker = getPhaseTracker();

        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.WRITE );
        sleep( 100 );
        phaseTracker.stop();

        EnumMap<PhaseTracker.Phase,LoggingPhaseTracker.Logger> times = phaseTracker.times();
        for ( Map.Entry<PhaseTracker.Phase,LoggingPhaseTracker.Logger> phaseEntry : times.entrySet() )
        {
            LoggingPhaseTracker.Logger logger = phaseEntry.getValue();
            PhaseTracker.Phase phase = phaseEntry.getKey();
            if ( phase == PhaseTracker.Phase.SCAN || phase == PhaseTracker.Phase.WRITE )
            {
                assertTrue( logger.totalTime >= 100 );
                assertTrue( logger.totalTime < 500 );
            }
            else
            {
                assertEquals( 0, logger.totalTime );
            }
        }
    }

    @Test
    void shouldAccumulateTimes()
    {
        LoggingPhaseTracker phaseTracker = getPhaseTracker();

        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.WRITE );
        LoggingPhaseTracker.Logger scanLogger = phaseTracker.times().get( PhaseTracker.Phase.SCAN );
        long firstCount = scanLogger.totalTime;
        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.stop();

        assertTrue( scanLogger.totalTime > firstCount );
    }

    @Test
    void throwIfEnterAfterStop()
    {
        PhaseTracker phaseTracker = getPhaseTracker();
        phaseTracker.stop();
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> phaseTracker.enterPhase( PhaseTracker.Phase.SCAN ) );
        assertThat( exception.getMessage(), containsString( "Trying to report a new phase after phase tracker has been stopped." ) );
    }

    @Test
    void mustReportMain()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        Log log = logProvider.getLog( IndexPopulationJob.class );
        PhaseTracker phaseTracker = getPhaseTracker( log );
        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.WRITE );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.WRITE );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.MERGE );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.BUILD );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.APPLY_EXTERNAL );
        sleep( 100 );
        phaseTracker.enterPhase( PhaseTracker.Phase.FLIP );
        sleep( 100 );

        // when
        phaseTracker.stop();

        // then
        AssertableLogProvider.LogMatcher logMatcher = AssertableLogProvider.inLog( IndexPopulationJob.class ).info(
                "TIME/PHASE Final: " +
                        "SCAN[totalTime=200ms, avgTime=100ms, minTime=0ns, maxTime=100ms, nbrOfReports=2], " +
                        "WRITE[totalTime=200ms, avgTime=100ms, minTime=0ns, maxTime=100ms, nbrOfReports=2], " +
                        "MERGE[totalTime=100ms], BUILD[totalTime=100ms], APPLY_EXTERNAL[totalTime=100ms], FLIP[totalTime=100ms]" );
        logProvider.assertAtLeastOnce( logMatcher );
    }

    @Test
    void mustReportPeriod()
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        Log log = logProvider.getLog( IndexPopulationJob.class );
        PhaseTracker phaseTracker = getPhaseTracker( 1, log );
        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );

        // when
        sleep( 1000 );
        phaseTracker.enterPhase( PhaseTracker.Phase.WRITE );

        // then
        AssertableLogProvider.LogMatcher firstEntry =
                AssertableLogProvider.inLog( IndexPopulationJob.class ).debug( "TIME/PHASE Total: SCAN[totalTime=1s], Last 1 sec: SCAN[totalTime=1s]" );
        logProvider.assertExactly( firstEntry );

        // when
        sleep( 1000 );
        phaseTracker.enterPhase( PhaseTracker.Phase.SCAN );

        // then
        AssertableLogProvider.LogMatcher secondEntry =
                AssertableLogProvider.inLog( IndexPopulationJob.class )
                        .debug( "TIME/PHASE Total: SCAN[totalTime=1s], WRITE[totalTime=1s], Last 1 sec: WRITE[totalTime=1s]" );
        logProvider.assertExactly( firstEntry, secondEntry );

        // when
        sleep( 1000 );
        phaseTracker.enterPhase( PhaseTracker.Phase.WRITE );

        // then
        AssertableLogProvider.LogMatcher thirdEntry =
                AssertableLogProvider.inLog( IndexPopulationJob.class )
                        .debug( "TIME/PHASE Total: " +
                                "SCAN[totalTime=2s, avgTime=1s, minTime=0ns, maxTime=1s, nbrOfReports=2], " +
                                "WRITE[totalTime=1s], " +
                                "Last 1 sec: SCAN[totalTime=1s]" );
        logProvider.assertExactly( firstEntry, secondEntry, thirdEntry );
    }

    private LoggingPhaseTracker getPhaseTracker()
    {
        return getPhaseTracker( NullLog.getInstance() );
    }

    private LoggingPhaseTracker getPhaseTracker( Log log )
    {
        return getPhaseTracker( PERIOD_INTERVAL, log );
    }

    private LoggingPhaseTracker getPhaseTracker( int periodIntervalInSeconds, Log log )
    {
        return new LoggingPhaseTracker( periodIntervalInSeconds, log, clock );
    }

    private void sleep( int i )
    {
        clock.forward( i, TimeUnit.MILLISECONDS );
    }
}
