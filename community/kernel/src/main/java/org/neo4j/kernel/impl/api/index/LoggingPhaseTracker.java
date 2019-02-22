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

import java.time.Clock;
import java.util.EnumMap;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.TimeUtil;
import org.neo4j.logging.Log;
import org.neo4j.util.FeatureToggles;
import org.neo4j.util.VisibleForTesting;

public class LoggingPhaseTracker implements PhaseTracker
{
    private static final String MESSAGE_PREFIX = "TIME/PHASE ";
    static final int PERIOD_INTERVAL = FeatureToggles.getInteger( LoggingPhaseTracker.class, "period_interval", 600 );

    private final long periodInterval;
    private final Log log;
    private final Clock clock;

    private EnumMap<Phase,Logger> times = new EnumMap<>( Phase.class );
    private Phase currentPhase;
    private long timeEnterPhase;
    private boolean stopped;
    private long lastPeriodReport = -1;

    LoggingPhaseTracker( Log log )
    {
        this( PERIOD_INTERVAL, log, Clock.systemUTC() );
    }

    @VisibleForTesting
    LoggingPhaseTracker( long periodIntervalInSeconds, Log log, Clock clock )
    {
        this.periodInterval = TimeUnit.SECONDS.toMillis( periodIntervalInSeconds );
        this.log = log;
        this.clock = clock;
        for ( Phase phase : Phase.values() )
        {
            times.put( phase, new Logger( phase ) );
        }
    }

    @Override
    public void enterPhase( Phase phase )
    {
        if ( stopped )
        {
            throw new IllegalStateException( "Trying to report a new phase after phase tracker has been stopped." );
        }
        if ( phase != currentPhase )
        {
            long now = logCurrentTime();
            currentPhase = phase;
            timeEnterPhase = now;

            if ( lastPeriodReport == -1 )
            {
                lastPeriodReport = now;
            }

            long millisSinceLastPeriodReport = now - lastPeriodReport;
            if ( millisSinceLastPeriodReport >= periodInterval )
            {
                // Report period
                periodReport( millisSinceLastPeriodReport );
                lastPeriodReport = now;
            }
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
        logCurrentTime();
        currentPhase = null;
        finalReport();
    }

    EnumMap<Phase,Logger> times()
    {
        return times;
    }

    private void finalReport()
    {
        log.info( MESSAGE_PREFIX + mainReportString( "Final" ) );
    }

    private void periodReport( long millisSinceLastPerioReport )
    {
        String periodReportString = periodReportString( millisSinceLastPerioReport );
        String mainReportString = mainReportString( "Total" );
        log.debug( MESSAGE_PREFIX + mainReportString + ", " + periodReportString );
    }

    private String mainReportString( String title )
    {
        StringJoiner joiner = new StringJoiner( ", ", title + ": ", "" );
        times.values().forEach( logger ->
        {
            reportToJoiner( joiner, logger );
        } );
        return joiner.toString();
    }

    private String periodReportString( long millisSinceLastPeriodReport )
    {
        long secondsSinceLastPeriodReport = TimeUnit.MILLISECONDS.toSeconds( millisSinceLastPeriodReport );
        StringJoiner joiner = new StringJoiner( ", ", "Last " + secondsSinceLastPeriodReport + " sec: ", "" );
        times.values().stream()
                .map( Logger::period )
                .forEach( period ->
                {
                    reportToJoiner( joiner, period );
                    period.reset();

                } );
        return joiner.toString();
    }

    private void reportToJoiner( StringJoiner joiner, Counter counter )
    {
        if ( counter.nbrOfReports > 0 )
        {
            joiner.add( counter.toString() );
        }
    }

    private long logCurrentTime()
    {
        long now = clock.millis();
        if ( currentPhase != null )
        {
            Logger logger = times.get( currentPhase );
            long timeMillis = now - timeEnterPhase;
            logger.log( timeMillis );
        }
        return now;
    }

    public class Logger extends Counter
    {
        final Counter periodCounter;

        private Logger( Phase phase )
        {
            super( phase );
            periodCounter = new Counter( phase );
            periodCounter.reset();
        }

        void log( long timeMillis )
        {
            super.log( timeMillis );
            periodCounter.log( timeMillis );
        }

        Counter period()
        {
            return periodCounter;
        }
    }

    public class Counter
    {
        private final Phase phase;
        long totalTime;
        long nbrOfReports;
        long maxTime;
        long minTime;

        Counter( Phase phase )
        {
            this.phase = phase;
        }

        void log( long timeMillis )
        {
            totalTime += timeMillis;
            nbrOfReports++;
            maxTime = Math.max( maxTime, timeMillis );
            minTime = Math.min( minTime, timeMillis );
        }

        void reset()
        {
            totalTime = 0;
            nbrOfReports = 0;
            maxTime = Long.MIN_VALUE;
            minTime = Long.MAX_VALUE;
        }

        @Override
        public String toString()
        {
            StringJoiner joiner = new StringJoiner( ", ", phase.toString() + "[", "]" );
            if ( nbrOfReports == 0 )
            {
                addToString( "nbrOfReports", nbrOfReports, joiner, false );
            }
            else if ( nbrOfReports == 1 )
            {
                addToString( "totalTime", totalTime, joiner, true );
            }
            else
            {
                long avgTime = totalTime / nbrOfReports;
                addToString( "totalTime", totalTime, joiner, true );
                addToString( "avgTime", avgTime, joiner, true );
                addToString( "minTime", minTime, joiner, true );
                addToString( "maxTime", maxTime, joiner, true );
                addToString( "nbrOfReports", nbrOfReports, joiner, false );
            }
            return joiner.toString();
        }

        void addToString( String name, long measurement, StringJoiner joiner, boolean isTime )
        {
            String measurementString;
            if ( isTime )
            {
                long timeInNanos = TimeUnit.MILLISECONDS.toNanos( measurement );
                measurementString = TimeUtil.nanosToString( timeInNanos );
            }
            else
            {
                measurementString = Long.toString( measurement );
            }
            joiner.add( String.format( "%s=%s", name, measurementString ) );
        }
    }
}
