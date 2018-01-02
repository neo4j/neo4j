/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.apache.commons.lang3.ArrayUtils;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;
import org.neo4j.logging.AssertableLogProvider;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsString;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@RunWith( Parameterized.class )
public class CappedLoggerTest
{

    public interface LogMethod
    {
        void log( CappedLogger logger, String msg, Throwable cause );
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> parameters()
    {
        LogMethod debug = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.debug( msg, cause );
            }
        };
        LogMethod info = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.info( msg, cause );
            }
        };
        LogMethod warn = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.warn( msg, cause );
            }
        };
        LogMethod error = new LogMethod()
        {
            @Override
            public void log( CappedLogger logger, String msg, Throwable cause )
            {
                logger.error( msg, cause );
            }
        };
        return Arrays.asList(
                new Object[]{ "debug", debug },
                new Object[] { "info", info },
                new Object[] { "warn", warn },
                new Object[] { "error", error }
        );
    }

    private static class ExceptionWithoutStackTrace extends Exception
    {
        public ExceptionWithoutStackTrace( String message )
        {
            super( message );
        }

        @Override
        public Throwable fillInStackTrace()
        {
            return this;
        }
    }

    private static class ExceptionWithoutStackTrace2 extends Exception
    {
        public ExceptionWithoutStackTrace2( String message )
        {
            super( message );
        }

        @Override
        public Throwable fillInStackTrace()
        {
            return this;
        }
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
            logMethod.log( logger, msg, null );
        }
        return lines;
    }

    public void assertLoggedLines( String[] lines, int count ) throws IOException
    {
        assertLoggedLines( lines, count, 0 );
    }

    public void assertLoggedLines( String[] lines, int count, int skip ) throws IOException
    {
        Matcher<String>[] matchers = new Matcher[count];
        int i;
        for ( i = 0; i < skip; i++ )
        {
            matchers[i] = any( String.class );
        }
        for (; i < count; i++ )
        {
            String line = lines[i];
            matchers[i] = containsString( line );
        }

        logProvider.assertContainsLogCallsMatching( skip, matchers );
    }

    @Before
    public void setUp()
    {
        logProvider = new AssertableLogProvider();
        logger = new CappedLogger( logProvider.getLog( CappedLogger.class ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowIfDelegateIsNull() throws Exception
    {
        new CappedLogger( null );
    }

    @Test
    public void mustLogWithoutLimitConfiguration() throws Exception
    {
        int lineCount = 1000;
        String[] lines = logLines( lineCount );
        assertLoggedLines( lines, lineCount );
    }

    @Test
    public void mustLogExceptions() throws Exception
    {
        logMethod.log( logger, "MESSAGE", new ArithmeticException( "EXCEPTION" ) );
        logProvider.assertContainsLogCallContaining( "MESSAGE" );
        logProvider.assertContainsLogCallContaining( "ArithmeticException" );
        logProvider.assertContainsLogCallContaining( "EXCEPTION" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnSettingZeroCountLimit() throws Exception
    {
        logger.setCountLimit( 0 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnSettingNegativeCountLimit() throws Exception
    {
        logger.setCountLimit( -1 );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnZeroTimeLimit() throws Exception
    {
        logger.setTimeLimit( 0, TimeUnit.MILLISECONDS, Clock.SYSTEM_CLOCK );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnNegativeTimeLimit() throws Exception
    {
        logger.setTimeLimit( -1, TimeUnit.MILLISECONDS, Clock.SYSTEM_CLOCK );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnNullTimeUnit() throws Exception
    {
        logger.setTimeLimit( 10, null, Clock.SYSTEM_CLOCK );
    }

    @Test( expected = IllegalArgumentException.class )
    public void mustThrowOnNullClock() throws Exception
    {
        logger.setTimeLimit( 10, TimeUnit.MILLISECONDS, null );
    }

    @Test
    public void mustAllowConfigurationChaining() throws Exception
    {
        logger.setCountLimit( 1 )
              .setTimeLimit( 10, TimeUnit.MILLISECONDS, Clock.SYSTEM_CLOCK )
              .setDuplicateFilterEnabled( true )
              .unsetCountLimit()
              .unsetTimeLimit()
              .setCountLimit( 1 );
    }

    @Test
    public void mustLimitByConfiguredCount() throws Exception
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        assertLoggedLines( lines, limit );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( lines[limit] ) ) );
    }

    @Test
    public void mustLogAfterResetWithCountLimit() throws Exception
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        logger.reset();
        String[] moreLines = logLines( 1, limit + 1 );
        assertLoggedLines( ArrayUtils.addAll( ArrayUtils.subarray( lines, 0, limit ), moreLines ), 1 + limit );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( lines[limit] ) ) );
        logProvider.assertContainsMessageMatching( containsString( moreLines[0] ) );
    }

    @Test
    public void unsettingCountLimitMustLetMessagesThrough() throws Exception
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
    public void mustNotLogMessagesWithinConfiguredTimeLimit() throws Exception
    {
        FakeClock clock = new FakeClock( 1000, TimeUnit.MILLISECONDS );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###", null );

        logProvider.assertContainsMessageMatching( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.assertContainsMessageMatching( containsString( "### CCC ###" ) );
    }

    @Test
    public void unsettingTimeLimitMustLetMessagesThrough() throws Exception
    {
        FakeClock clock = new FakeClock( 1000, TimeUnit.MILLISECONDS );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###", null );
        logMethod.log( logger, "### DDD ###", null );
        logger.unsetTimeLimit(); // Note that we are not advancing the clock!
        logMethod.log( logger, "### EEE ###", null );

        logProvider.assertContainsMessageMatching( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.assertContainsMessageMatching( containsString( "### CCC ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### DDD ###" ) ) );
        logProvider.assertContainsMessageMatching( containsString( "### EEE ###" ) );
    }

    @Test
    public void mustLogAfterResetWithTimeLimit() throws Exception
    {
        FakeClock clock = new FakeClock( 1000, TimeUnit.MILLISECONDS );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null );
        logger.reset();
        logMethod.log( logger, "### CCC ###", null );

        logProvider.assertContainsMessageMatching( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.assertContainsMessageMatching( containsString( "### CCC ###" ) );
    }

    @Test
    public void mustOnlyLogMessagesThatPassBothLimits() throws Exception
    {
        FakeClock clock = new FakeClock( 1000, TimeUnit.MILLISECONDS );
        logger.setCountLimit( 2 );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null ); // Filtered by the time limit
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###", null ); // Filtered by the count limit
        logger.reset();
        logMethod.log( logger, "### DDD ###", null );

        logProvider.assertContainsMessageMatching( containsString( "### AAA ###" ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### BBB ###" ) ) );
        logProvider.assertNone( currentLog( inLog( CappedLogger.class ), containsString( "### CCC ###" ) ) );
        logProvider.assertContainsMessageMatching( containsString( "### DDD ###" ) );
    }

    @Test
    public void mustFilterDuplicateMessageAndNullException() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### AAA ###", null ); // duplicate
        logMethod.log( logger, "### BBB ###", null );
        String[] lines = new String[]{"### AAA ###", "### BBB ###"};
        assertLoggedLines( lines, lines.length );
    }

    @Test
    public void mustFilterDuplicateMessageAndException() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_aaa" ) );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_aaa" ) ); // duplicate
        logMethod.log( logger, "### BBB ###", new ExceptionWithoutStackTrace( "exc_bbb" ) );

        String[] messages = new String[]{"### AAA ###", "### BBB ###"};
        assertLoggedLines( messages, messages.length );
    }

    @Test
    public void mustLogSameMessageAndDifferentExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_aaa" ) );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_bbb" ) ); // Different message
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace2( "exc_bbb" ) ); // Different type

        String[] messages = new String[]{"### AAA ###", "### AAA ###", "### AAA ###"};
        assertLoggedLines( messages, messages.length );
    }

    @Test
    public void mustLogSameMessageAndNonNullExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( null ) ); // Different message
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace2( null ) ); // Different type

        String[] messages = new String[]{"### AAA ###", "### AAA ###", "### AAA ###"};
        assertLoggedLines( messages, messages.length );
    }

    @Test
    public void mustFilterSameMessageAndExceptionWithNullMessage() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( null ) );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( null ) );
        logMethod.log( logger, "### BBB ###", null );

        String[] messages = new String[]{"### AAA ###", "### BBB ###"};
        assertLoggedLines( messages, messages.length );
    }

    @Test
    public void mustLogDifferentMessageAndSameExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "xyz" ) );
        logMethod.log( logger, "### BBB ###", new ExceptionWithoutStackTrace( "xyz" ) );

        String[] messages = new String[]{"### AAA ###", "### BBB ###"};
        assertLoggedLines( messages, messages.length );
    }

    @Test
    public void mustLogDifferentMessageAndDifferentExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "foo" ) );
        logMethod.log( logger, "### BBB ###", new ExceptionWithoutStackTrace( "bar" ) );

        String[] messages = new String[]{"### AAA ###", "### BBB ###"};
        assertLoggedLines( messages, messages.length );
    }

    @Test
    public void mustLogSameMessageAndExceptionAfterResetWithDuplicateFilter() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "xyz" ) );
        logger.reset();
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "xyz" ) );

        String[] messages = new String[]{"### AAA ###", "### AAA ###"};
        assertLoggedLines( messages, messages.length );
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
}
