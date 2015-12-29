/*
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
package org.neo4j.kernel.impl.util;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.helpers.FakeClock;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

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

    private final LogMethod logMethod;

    private StringBuffer buffer;
    private CappedLogger logger;

    public CappedLoggerTest( @SuppressWarnings( "UnusedParameters" ) String ignoreTestName, LogMethod logMethod )
    {
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

    public void assertLoggedLines( String[] lines, int offset, int count ) throws IOException
    {
        assertLoggedLines( lines, offset, count, 0 );
    }

    public void assertLoggedLines( String[] lines, int offset, int count, int skip ) throws IOException
    {
        String output = buffer.toString();
        BufferedReader reader = new BufferedReader( new StringReader( output ) );
        for ( int i = 0; i < skip; i++ )
        {
            reader.readLine();
        }
        for ( int i = offset; i < count; i++ )
        {
            String line = lines[i];
            assertThat( reader.readLine(), containsString( line ) );
        }
    }

    @Before
    public void setUp()
    {
        buffer = new StringBuffer();
        StringLogger delegate = StringLogger.wrap( buffer, true );
        logger = new CappedLogger( delegate );
    }

    @Test
    public void mustLogWithoutLimitConfiguration() throws Exception
    {
        int lineCount = 1000;
        String[] lines = logLines( lineCount );
        assertLoggedLines( lines, 0, lineCount );
    }

    @Test
    public void mustLogExceptions() throws Exception
    {
        logMethod.log( logger, "MESSAGE", new ArithmeticException( "EXCEPTION" ) );
        String output = buffer.toString();
        assertThat( output, containsString( "MESSAGE" ) );
        assertThat( output, containsString( "ArithmeticException" ) );
        assertThat( output, containsString( "EXCEPTION" ) );
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
    public void mustLimitByConfiguredCount() throws Exception
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        assertLoggedLines( lines, 0, limit );
        assertThat( buffer.toString(), not( containsString( lines[limit] ) ) );
    }

    @Test
    public void mustLogAfterResetWithCountLimit() throws Exception
    {
        int limit = 10;
        logger.setCountLimit( limit );
        String[] lines = logLines( limit + 1 );
        logger.reset();
        String[] moreLines = logLines( 1, limit + 1 );
        assertLoggedLines( lines, 0, limit );
        assertThat( buffer.toString(), not( containsString( lines[limit] ) ) );
        assertThat( buffer.toString(), containsString( moreLines[0] ) );
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
        assertLoggedLines( lines, 0, limit );
        assertThat( buffer.toString(), not( containsString( lines[limit] ) ) );
        assertLoggedLines( moreLines, 0, moreLineCount, limit );
    }

    @Test
    public void mustNotLogMessagesWithinConfiguredTimeLimit() throws Exception
    {
        FakeClock clock = new FakeClock( 1000 );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###", null );

        String output = buffer.toString();
        assertThat( output, containsString( "### AAA ###" ) );
        assertThat( output, not( containsString( "### BBB ###" ) ) );
        assertThat( output, containsString( "### CCC ###" ) );
    }

    @Test
    public void unsettingTimeLimitMustLetMessagesThrough() throws Exception
    {
        FakeClock clock = new FakeClock( 1000 );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null );
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###", null );
        logMethod.log( logger, "### DDD ###", null );
        logger.unsetTimeLimit(); // Note that we are not advancing the clock!
        logMethod.log( logger, "### EEE ###", null );

        String output = buffer.toString();
        assertThat( output, containsString( "### AAA ###" ) );
        assertThat( output, not( containsString( "### BBB ###" ) ) );
        assertThat( output, containsString( "### CCC ###" ) );
        assertThat( output, not( containsString( "### DDD ###" ) ) );
        assertThat( output, containsString( "### EEE ###" ) );
    }

    @Test
    public void mustLogAfterResetWithTimeLimit() throws Exception
    {
        FakeClock clock = new FakeClock( 1000 );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null );
        logger.reset();
        logMethod.log( logger, "### CCC ###", null );

        String output = buffer.toString();
        assertThat( output, containsString( "### AAA ###" ) );
        assertThat( output, not( containsString( "### BBB ###" ) ) );
        assertThat( output, containsString( "### CCC ###" ) );
    }

    @Test
    public void mustOnlyLogMessagesThatPassBothLimits() throws Exception
    {
        FakeClock clock = new FakeClock( 1000 );
        logger.setCountLimit( 2 );
        logger.setTimeLimit( 1, TimeUnit.MILLISECONDS, clock );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### BBB ###", null ); // Filtered by the time limit
        clock.forward( 1, TimeUnit.MILLISECONDS );
        logMethod.log( logger, "### CCC ###", null ); // Filtered by the count limit
        logger.reset();
        logMethod.log( logger, "### DDD ###", null );

        String output = buffer.toString();
        assertThat( output, containsString( "### AAA ###" ) );
        assertThat( output, not( containsString( "### BBB ###" ) ) );
        assertThat( output, not( containsString( "### CCC ###" ) ) );
        assertThat( output, containsString( "### DDD ###" ) );
    }

    @Test
    public void mustFilterDuplicateMessageAndNullException() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### AAA ###", null ); // duplicate
        logMethod.log( logger, "### BBB ###", null );
        String[] lines = new String[] { "### AAA ###", "### BBB ###" };
        assertLoggedLines( lines, 0, lines.length );
    }

    @Test
    public void mustFilterDuplicateMessageAndException() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_aaa" ) );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_aaa" ) ); // duplicate
        logMethod.log( logger, "### BBB ###", new ExceptionWithoutStackTrace( "exc_bbb" ) );

        String[] messages = new String[] { "### AAA ###", "exc_aaa", "### BBB ###", "exc_bbb" };
        assertLoggedLines( messages, 0, messages.length );
    }

    @Test
    public void mustLogSameMessageAndDifferentExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_aaa" ) );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "exc_bbb" ) ); // Different message
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace2( "exc_bbb" ) ); // Different type

        String[] messages = new String[] {
                "### AAA ###", "exc_aaa", "### AAA ###", "exc_bbb", "### AAA ###", "exc_bbb" };
        assertLoggedLines( messages, 0, messages.length );
    }

    @Test
    public void mustLogSameMessageAndNonNullExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", null );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( null ) ); // Different message
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace2( null ) ); // Different type

        String[] messages = new String[] {
                "### AAA ###", "### AAA ###", "ExceptionWithoutStackTrace",
                "### AAA ###", "ExceptionWithoutStackTrace2" };
        assertLoggedLines( messages, 0, messages.length );
    }

    @Test
    public void mustFilterSameMessageAndExceptionWithNullMessage() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( null ) );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( null ) );
        logMethod.log( logger, "### BBB ###", null );

        String[] messages = new String[] {
                "### AAA ###", "ExceptionWithoutStackTrace", "### BBB ###" };
        assertLoggedLines( messages, 0, messages.length );
    }

    @Test
    public void mustLogDifferentMessageAndSameExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "xyz" ) );
        logMethod.log( logger, "### BBB ###", new ExceptionWithoutStackTrace( "xyz" ) );

        String[] messages = new String[] {
                "### AAA ###", "xyz",
                "### BBB ###", "xyz" };
        assertLoggedLines( messages, 0, messages.length );
    }

    @Test
    public void mustLogDifferentMessageAndDifferentExceptionWithDuplicateLimit() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "foo" ) );
        logMethod.log( logger, "### BBB ###", new ExceptionWithoutStackTrace( "bar" ) );

        String[] messages = new String[] {
                "### AAA ###", "foo",
                "### BBB ###", "bar" };
        assertLoggedLines( messages, 0, messages.length );
    }

    @Test
    public void mustLogSameMessageAndExceptionAfterResetWithDuplicateFilter() throws Exception
    {
        logger.setDuplicateFilterEnabled( true );
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "xyz" ) );
        logger.reset();
        logMethod.log( logger, "### AAA ###", new ExceptionWithoutStackTrace( "xyz" ) );

        String[] messages = new String[] {
                "### AAA ###", "xyz",
                "### AAA ###", "xyz" };
        assertLoggedLines( messages, 0, messages.length );
    }
}
