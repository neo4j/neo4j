/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.logging;

import org.junit.jupiter.api.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.IllegalFormatException;
import java.util.function.Supplier;

import org.neo4j.function.Suppliers;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormattedLogTest
{
    private static final Supplier<ZonedDateTime> DATE_TIME_SUPPLIER = () ->
            ZonedDateTime.of( 1984, 10, 26, 4, 23, 24, 343000000, ZoneOffset.UTC );

    @Test
    void logShouldWriteMessageStandard()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Terminator 2" );

        // Then
        assertThat( writer.toString() ).isEqualTo( format( "1984-10-26 04:23:24.343+0000 INFO [test] Terminator 2%n" ) );
    }

    @Test
    void logShouldWriteMessageJson()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer, Level.DEBUG, FormattedLogFormat.JSON_FORMAT );

        // When
        log.info( "Terminator 2" );

        // Then
        assertThat( writer.toString() ).isEqualTo(
                format( "{\"time\": \"1984-10-26 04:23:24.343+0000\", \"level\": \"INFO\", \"category\": \"test\", \"message\": \"Terminator 2\"}%n" ) );
    }

    @Test
    void logShouldWriteMessageAndThrowableStandard()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Hasta la vista, baby", newThrowable( "<message>", "<stacktrace>" ) );

        // Then
        assertThat( writer.toString() ).isEqualTo( format( "1984-10-26 04:23:24.343+0000 INFO [test] Hasta la vista, baby " + "<message>%n<stacktrace>" ) );
    }

    @Test
    void logShouldWriteMessageAndThrowableJson()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer, Level.DEBUG, FormattedLogFormat.JSON_FORMAT );

        // When
        log.info( "Hasta la vista, baby", newThrowable( "<message>", "<stacktrace>" ) );

        // Then
        assertThat( writer.toString() ).isEqualTo(
                "{\"time\": \"1984-10-26 04:23:24.343+0000\", \"level\": \"INFO\", \"category\": \"test\", " +
                        "\"message\": \"Hasta la vista, baby\", \"stacktraceMessage\": \"<message>\", \"stacktrace\": \"<stacktrace>\"}" );
    }

    @Test
    void logShouldWriteMessageAndThrowableWithNullMessageStandard()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Hasta la vista, baby", newThrowable( null, "<stacktrace>" ) );

        // Then
        assertThat( writer.toString() ).isEqualTo( format( "1984-10-26 04:23:24.343+0000 INFO [test] Hasta la vista, baby%n<stacktrace>" ) );
    }

    @Test
    void logShouldWriteMessageAndThrowableWithNullMessageJson()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer, Level.DEBUG, FormattedLogFormat.JSON_FORMAT );

        // When
        log.info( "Hasta la vista, baby", newThrowable( null, "<stacktrace>" ) );

        // Then
        assertThat( writer.toString() ).isEqualTo(
                "{\"time\": \"1984-10-26 04:23:24.343+0000\", \"level\": \"INFO\", \"category\": \"test\", \"message\": " +
                        "\"Hasta la vista, baby\", \"stacktrace\": \"<stacktrace>\"}" );
    }

    @Test
    void logShouldWriteMessageWithFormat()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "I need your %s, your %s and your %s", "clothes", "boots", "motorcycle" );

        // Then
        assertThat( writer.toString() ).isEqualTo(
                format( "1984-10-26 04:23:24.343+0000 INFO [test] I need your clothes, your boots and your " + "motorcycle%n" ) );
    }

    @Test
    void logShouldWriteNotFormattedMessageWhenNoParametersGiven()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( "Come with me if you %s to live!", new Object[]{} );

        // Then
        assertThat( writer.toString() ).isEqualTo( format( "1984-10-26 04:23:24.343+0000 INFO [test] Come with me if you %%s to live!%n" ) );
    }

    @Test
    void logShouldFailAndWriteNothingForInvalidParametersArray()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        assertThrows( IllegalFormatException.class, () -> log.info( "%s like me. A T-%d, advanced prototype.", "Not", "1000", 1000 ) );
        assertThat( writer.toString() ).isEqualTo( "" );
    }

    @Test
    void shouldNotWriteLogIfLevelIsHigherThanWritten()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer, Level.WARN );

        // When
        log.info( "I know now why you cry. But it's something I can never do." );

        // Then
        assertThat( writer.toString() ).isEqualTo( "" );
    }

    @Test
    void shouldAllowLevelToBeChanged()
    {
        // Given
        StringWriter writer = new StringWriter();
        FormattedLog log = newFormattedLog( writer, Level.INFO);

        // When
        log.info( "No, it's when there's nothing wrong with you, but you hurt anyway. You get it?" );
        log.setLevel( Level.WARN );
        log.info( "I know now why you cry. But it's something I can never do." );
        log.setLevel( Level.DEBUG );
        log.info( "There's 215 bones in the human body. That's one." );

        // Then
        assertThat( writer.toString() ).isEqualTo( format( "%s%n%s%n",
                "1984-10-26 04:23:24.343+0000 INFO [test] No, it's when there's nothing wrong with you, but " + "you hurt anyway. You get it?",
                "1984-10-26 04:23:24.343+0000 INFO [test] There's 215 bones in the human body. That's one." ) );
    }

    private static FormattedLog newFormattedLog( StringWriter writer )
    {
        return newFormattedLog( writer, Level.DEBUG );
    }

    static FormattedLog newFormattedLog( StringWriter writer, Level level )
    {
        return newFormattedLog( writer, level, FormattedLogFormat.STANDARD_FORMAT );
    }

    static FormattedLog newFormattedLog( StringWriter writer, Level level, FormattedLogFormat format )
    {
        return FormattedLog
                .withUTCTimeZone()
                .withCategory( "test" )
                .withLogLevel( level )
                .withTimeSupplier( DATE_TIME_SUPPLIER )
                .withFormat( format )
                .toPrintWriter( Suppliers.singleton( new PrintWriter( writer ) ) );
    }

    static Throwable newThrowable( final String message, final String stackTrace )
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