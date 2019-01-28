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

import org.junit.Test;

import java.util.EnumMap;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static java.lang.Thread.sleep;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LoggingPhaseTrackerTest
{
    @Test
    public void shouldLogSingleTime() throws InterruptedException
    {
        LoggingPhaseTracker phaseTracker = getPhaseTracker();

        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.stop();

        EnumMap<LoggingPhaseTracker.Phase,LoggingPhaseTracker.Logger> times = phaseTracker.times();
        for ( LoggingPhaseTracker.Phase phase : times.keySet() )
        {
            LoggingPhaseTracker.Logger logger = times.get( phase );
            if ( phase == LoggingPhaseTracker.Phase.SCAN )
            {
                assertTrue( logger.totalTime >= TimeUnit.MILLISECONDS.toNanos( 100 ) );
                assertTrue( logger.totalTime < TimeUnit.MILLISECONDS.toNanos( 500 ) );
            }
            else
            {
                assertEquals( 0, logger.totalTime );
            }
        }
    }

    @Test
    public void shouldLogMultipleTimes() throws InterruptedException
    {
        LoggingPhaseTracker phaseTracker = getPhaseTracker();

        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.WRITE );
        sleep( 100 );
        phaseTracker.stop();

        EnumMap<LoggingPhaseTracker.Phase,LoggingPhaseTracker.Logger> times = phaseTracker.times();
        for ( LoggingPhaseTracker.Phase phase : times.keySet() )
        {
            LoggingPhaseTracker.Logger logger = times.get( phase );
            if ( phase == LoggingPhaseTracker.Phase.SCAN ||
                    phase == LoggingPhaseTracker.Phase.WRITE )
            {
                assertTrue( logger.totalTime >= TimeUnit.MILLISECONDS.toNanos( 100 ) );
                assertTrue( logger.totalTime < TimeUnit.MILLISECONDS.toNanos( 500 ) );
            }
            else
            {
                assertEquals( 0, logger.totalTime );
            }
        }
    }

    @Test
    public void shouldAccumulateTimes() throws InterruptedException
    {
        LoggingPhaseTracker phaseTracker = getPhaseTracker();

        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.WRITE );
        LoggingPhaseTracker.Logger scanLogger = phaseTracker.times().get( LoggingPhaseTracker.Phase.SCAN );
        long firstCount = scanLogger.totalTime;
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.stop();

        assertTrue( scanLogger.totalTime > firstCount );
    }

    @Test
    public void throwIfEnterAfterStop()
    {
        PhaseTracker phaseTracker = getPhaseTracker();
        phaseTracker.stop();
        try
        {
            phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );
            fail( "Should have failed" );
        }
        catch ( IllegalStateException e )
        {
            assertThat( e.getMessage(), containsString( "Trying to report a new phase after phase tracker has been stopped." ) );
        }
    }

    @Test
    public void mustReportMain() throws InterruptedException
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        Log log = logProvider.getLog( IndexPopulationJob.class );
        PhaseTracker phaseTracker = getPhaseTracker( log );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );
        sleep( 100 );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.WRITE );
        sleep( 100 );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.FLIP );
        sleep( 100 );

        // when
        phaseTracker.stop();

        // then
        //noinspection unchecked
        logProvider.assertContainsMessageMatching( allOf(
                containsString( "TIME/PHASE" ),
                containsString( "Final: " ),
                containsString( "SCAN" ),
                containsString( "WRITE" ),
                containsString( "FLIP" ),
                containsString( "totalTime=" ),
                containsString( "avgTime=" ),
                containsString( "minTime=" ),
                containsString( "maxTime=" ),
                containsString( "nbrOfReports=" )
        ) );
    }

    @Test
    public void mustReportPeriod() throws InterruptedException
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider( true );
        Log log = logProvider.getLog( IndexPopulationJob.class );
        PhaseTracker phaseTracker = getPhaseTracker( 1, log );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.SCAN );

        // when
        sleep( 1000 );
        phaseTracker.enterPhase( LoggingPhaseTracker.Phase.WRITE );

        // then
        //noinspection unchecked
        logProvider.assertContainsMessageMatching( allOf(
                containsString( "TIME/PHASE" ),
                containsString( "Total:" ),
                containsString( "SCAN" ),
                containsString( "WRITE" ),
                containsString( "FLIP" ),
                containsString( "totalTime=" ),
                containsString( "avgTime=" ),
                containsString( "minTime=" ),
                containsString( "maxTime=" ),
                containsString( "nbrOfReports=" )
        ) );
    }

    private LoggingPhaseTracker getPhaseTracker()
    {
        return getPhaseTracker( NullLog.getInstance() );
    }

    private LoggingPhaseTracker getPhaseTracker( Log log )
    {
        return new LoggingPhaseTracker( log );
    }

    private PhaseTracker getPhaseTracker( int periodIntervalInSeconds, Log log )
    {
        return new LoggingPhaseTracker( periodIntervalInSeconds, log );
    }
}
