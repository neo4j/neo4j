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
package org.neo4j.logging.internal;

import org.apache.commons.lang3.ArrayUtils;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@RunWith( Parameterized.class )
public class CappedLoggerTest
{

    public interface LogMethod
    {
        void log( CappedLogger logger, String msg );

        void log( CappedLogger logger, String msg, Throwable cause );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Iterable<Object[]> parameters()
    {
        LogMethod debug = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg )
            {
                logger.debug( msg );
            }

            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.debug( msg, cause );
            }
        };
        LogMethod info = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg )
            {
                logger.debug( msg );
            }

            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.info( msg, cause );
            }
        };
        LogMethod warn = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg )
            {
                logger.debug( msg );
            }

            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.warn( msg, cause );
            }
        };
        LogMethod error = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg )
            {
                logger.debug( msg );
            }

            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.error( msg, cause );
            }
        };
        return Arrays.asList(
                new Object[]{"debug", debug},
                new Object[]{"info", info},
                new Object[]{"warn", warn},
                new Object[]{"error", error}
        );
    }

    private final String logName;
    private final LogMethod logMethod;

    private AssertableLogProvider logProvider;
    private CappedLogger logger;

    public CappedLoggerTest( String logName, LogMethod logMethod )
    {
        this.logName = logName;
        this.logMethod = logMethod;
    }

    public String[] logLines( int lineCount )
    {
        return logLines( lineCount, 0 );
    }

    public String[] logLines( int lineCount, int startAt )
    {
        String[] lines = new String[lineCount];
        for ( int i = 0; i < lineCount; i++ )
        {
            String msg = String.format( "### %04d ###", startAt + i );
            lines[i] = msg;
            logMethod.log( logger, msg );
        }
        return lines;
    }

    public void assertLoggedLines( String[] lines, int count )
    {
        assertLoggedLines( lines, count, 0 );
    }

    public void assertLoggedLines( String[] lines, int count, int skip )
    {
        Matcher<String>[] matchers = new Matcher[count];
        int i;
        for ( i = 0; i < skip; i++ )
        {
            matchers[i] = any( String.class );
        }
        for ( ; i < count; i++ )
        {
            String line = lines[i];
            matchers[i] = containsString( line );
        }

        logProvider.rawMessageMatcher().assertContains( skip, matchers );
    }

    @Before
    public void setUp()
    {
        logProvider = new AssertableLogProvider();
        logger = new CappedLogger( logProvider.getLog( CappedLogger.class ) );
    }

    @Test
    public void mustLogWithoutLimitConfiguration()
    {
        int lineCount = 1000;
        String[] lines = logLines( lineCount );
        assertLoggedLines( lines, lineCount );
    }

    @Test
    public void mustLogExceptions()
    {
        logMethod.log( logger, "MESSAGE", new ArithmeticException( "EXCEPTION" ) );
        AssertableLogProvider.MessageMatcher matcher = logProvider.internalToStringMessageMatcher();
        matcher.assertContains( "MESSAGE" );
        matcher.assertContains( "ArithmeticException" );
        matcher.assertContains( "EXCEPTION" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnSettingZeroCountLimit()
    {
        logger.setCountLimit( 0 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnSettingNegativeCountLimit()
    {
        logger.setCountLimit( -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnZeroTimeLimit()
    {
        logger.setTimeLimit( 0, MILLISECONDS, Clocks.systemClock() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnNegativeTimeLimit()
    {
        logger.setTimeLimit( -1, MILLISECONDS, Clocks.systemClock() );
    }

    @Test
    public void mustAllowConfigurationChaining()
    {
        logger.setCountLimit( 1 )
                .setTimeLimit( 10, MILLISECONDS, Clocks.systemClock() )
                .unsetCountLimit()
                .unsetTimeLimit()
                .setCountLimit( 1 );
    }

    @Test
    public void mustLimitByConfiguredCount()
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        assertLoggedLines( lines, limit );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( lines[limit] ) ) );
    }

    @Test
    public void mustLogAfterResetWithCountLimit()
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        logger.reset();
        String[] moreLines = logLines( 1, limit + 1 );
        assertLoggedLines( ArrayUtils.addAll( ArrayUtils.subarray( lines, 0, limit ), moreLines ), 1 + limit );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( lines[limit] ) ) );
        logProvider.rawMessageMatcher().assertContains( containsString( moreLines[0] ) );
    }

    @Test
    public void unsettingCountLimitMustLetMessagesThrough()
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        logger.unsetCountLimit();
        int moreLineCount = 1000;
        String[] moreLines = logLines( moreLineCount, limit + 1 );
        assertLoggedLines( ArrayUtils.addAll( ArrayUtils.subarray( lines, 0, limit ), moreLines ),
                moreLineCount + limit );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( lines[limit] ) ) );
        assertLoggedLines( moreLines, moreLineCount, limit );
    }

    @Test
    public void mustNotLogMessagesWithinConfiguredTimeLimit()
    {
        FakeClock clock = getDefaultFakeClock();
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###" );
        logMethod.log( logger, "### BBB ###" );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###" );

        logProvider.rawMessageMatcher().assertContains( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.rawMessageMatcher().assertContains( containsString( "### CCC ###" ) );
    }

    @Test
    public void unsettingTimeLimitMustLetMessagesThrough()
    {
        FakeClock clock = getDefaultFakeClock();
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###" );
        logMethod.log( logger, "### BBB ###" );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###" );
        logMethod.log( logger, "### DDD ###" );
        logger.unsetTimeLimit(); // Note that we are not advancing the clock!
        logMethod.log( logger, "### EEE ###" );

        logProvider.rawMessageMatcher().assertContains( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.rawMessageMatcher().assertContains( containsString( "### CCC ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### DDD ###" ) ) );
        logProvider.rawMessageMatcher().assertContains( containsString( "### EEE ###" ) );
    }

    @Test
    public void mustLogAfterResetWithTimeLimit()
    {
        FakeClock clock = getDefaultFakeClock();
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###" );
        logMethod.log( logger, "### BBB ###" );
        logger.reset();
        logMethod.log( logger, "### CCC ###" );

        logProvider.rawMessageMatcher().assertContains( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.rawMessageMatcher().assertContains( containsString( "### CCC ###" ) );
    }

    @Test
    public void mustOnlyLogMessagesThatPassBothLimits()
    {
        FakeClock clock = getDefaultFakeClock();
        logger.setCountLimit( 2 );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###" );
        logMethod.log( logger, "### BBB ###" ); // Filtered by the time limit
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###" ); // Filtered by the count limit
        logger.reset();
        logMethod.log( logger, "### DDD ###" );

        logProvider.rawMessageMatcher().assertContains( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### CCC ###" ) ) );
        logProvider.rawMessageMatcher().assertContains( containsString( "### DDD ###" ) );
    }

    private AssertableLogProvider.LogMatcher currentLog( AssertableLogProvider.LogMatcherBuilder logMatcherBuilder,
            Matcher<String> stringMatcher )
    {
        switch ( logName )
        {
        case "debug":
            return logMatcherBuilder.debug( stringMatcher );
        case "info":
            return logMatcherBuilder.info( stringMatcher );
        case "warn":
            return logMatcherBuilder.warn( stringMatcher );
        case "error":
            return logMatcherBuilder.error( stringMatcher );
        default:
            throw new RuntimeException( "Unknown log name" );
        }
    }

    private FakeClock getDefaultFakeClock()
    {
        return Clocks.fakeClock( 1000, TimeUnit.MILLISECONDS );
    }
}
