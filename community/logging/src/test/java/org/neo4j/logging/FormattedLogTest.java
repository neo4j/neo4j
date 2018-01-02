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
package org.neo4j.logging;

import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.IllegalFormatException;

import org.neo4j.function.Suppliers;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class FormattedLogTest
{
    private static final Date FIXED_DATE = new Date( 467612604343L );

    @Test
    public void logShouldWriteMessage()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Terminator 2" );

        // Then
        assertThat( writer.toString(), equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] Terminator 2%n" ) ) );
    }

    @Test
    public void logShouldFailAndWriteNothingForNullMessage()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        try
        {
            // When
            log.info( null );
            fail( "Should have thrown " + NullPointerException.class );
        } catch ( NullPointerException npe )
        {
            // Then
            assertThat( writer.toString(), equalTo( "" ) );
        }
    }

    @Test
    public void logShouldWriteMessageAndThrowable()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Hasta la vista, baby", newThrowable( "<message>", "<stacktrace>" ) );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] Hasta la vista, baby <message>%n<stacktrace>" ) )
        );
    }

    @Test
    public void logShouldFailAndWriteNothingForNullMessageAndNotNullThrowable()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        try
        {
            // When
            log.info( null, newThrowable( "<message>", "<stacktrace>" ) );
            fail( "Should have thrown " + NullPointerException.class );
        } catch ( NullPointerException npe )
        {
            // Then
            assertThat( writer.toString(), equalTo( "" ) );
        }
    }

    @Test
    public void logShouldWriteMessageForNotNullMessageAndNullThrowable()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "I'll take care of the police", (Throwable) null );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] I'll take care of the police%n" ) )
        );
    }

    @Test
    public void logShouldWriteMessageAndThrowableWithNullMessage()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Hasta la vista, baby", newThrowable( null, "<stacktrace>" ) );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] Hasta la vista, baby%n<stacktrace>" ) )
        );
    }

    @Test
    public void logShouldWriteMessageWithFormat()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "I need your %s, your %s and your %s", "clothes", "boots", "motorcycle" );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] I need your clothes, your boots and your motorcycle%n" ) )
        );
    }

    @Test
    public void logShouldFailAndWriteNothingForNullMessageFormat()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        try
        {
            // When
            log.info( null, "foo", "bar", "baz" );
            fail( "Should have thrown " + NullPointerException.class );
        } catch ( NullPointerException npe )
        {
            // Then
            assertThat( writer.toString(), equalTo( "" ) );
        }
    }

    @Test
    public void logShouldWriteNotFormattedMessageWhenNoParametersGiven()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Come with me if you %s to live!", new Object[]{} );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] Come with me if you %%s to live!%n" ) )
        );
    }

    @Test
    public void logShouldWriteNotFormattedMessageWhenParametersArrayIsNull()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Come with me if you %s to live!", (Object[]) null );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "1984-10-26 04:23:24.343+0000 INFO  [test] Come with me if you %%s to live!%n" ) )
        );
    }

    @Test
    public void logShouldFailAndWriteNothingForInvalidParametersArray()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        try
        {
            // When
            log.info( "%s like me. A T-%d, advanced prototype.", "Not", "1000", 1000 );
            fail( "Should have thrown " + IllegalFormatException.class );
        } catch ( IllegalFormatException ife )
        {
            // Then
            assertThat( writer.toString(), equalTo( "" ) );
        }
    }

    @Test
    public void shouldNotWriteLogIfLevelIsHigherThanWritten()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer, Level.WARN );

        // When
        log.info( "I know now why you cry. But it's something I can never do." );

        // Then
        assertThat( writer.toString(), equalTo( "" ) );
    }

    @Test
    public void shouldAllowLevelToBeChanged()
    {
        // Given
        StringWriter writer = new StringWriter();
        FormattedLog log = newFormattedLog( writer, Level.INFO );

        // When
        log.info( "No, it's when there's nothing wrong with you, but you hurt anyway. You get it?" );
        log.setLevel( Level.WARN );
        log.info( "I know now why you cry. But it's something I can never do." );
        log.setLevel( Level.DEBUG );
        log.info( "There's 215 bones in the human body. That's one." );

        // Then
        assertThat(
                writer.toString(),
                equalTo( format( "%s%n%s%n",
                        "1984-10-26 04:23:24.343+0000 INFO  [test] No, it's when there's nothing wrong with you, but you hurt anyway. You get it?",
                        "1984-10-26 04:23:24.343+0000 INFO  [test] There's 215 bones in the human body. That's one."
                ) )
        );
    }

    private static FormattedLog newFormattedLog( StringWriter writer )
    {
        return newFormattedLog( writer, Level.DEBUG );
    }

    private static FormattedLog newFormattedLog( StringWriter writer, Level level )
    {
        return new FormattedLog(
                Suppliers.singleton( FIXED_DATE ),Suppliers.singleton( new PrintWriter( writer ) ),
                FormattedLog.UTC, null, "test", level, true );
    }

    private static Throwable newThrowable( final String message, final String stackTrace )
    {
        return new Throwable()
        {
            @Override
            public void printStackTrace( PrintWriter s )
            {
                s.append( stackTrace );
            }

            @Override
            public String getMessage()
            {
                return message;
            }
        };
    }
}
