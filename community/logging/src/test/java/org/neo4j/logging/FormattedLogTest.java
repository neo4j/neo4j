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
package org.neo4j.logging;

import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.IllegalFormatException;

import org.neo4j.function.Suppliers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FormattedLogTest
{
    private static final Date FIXED_DATE = new Date( 467612604343L );

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForNullMessage()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = FormattedLog.toPrintWriter( new PrintWriter( writer ) );

        try
        {
            // When
            log.info( null );
            fail( "Should have thrown " + IllegalArgumentException.class );
        }
        catch ( IllegalArgumentException npe )
        {
            // Then
            assertEquals( "", writer.toString() );
        }
    }

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForNullMessageAndNotNullThrowable()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = FormattedLog.toPrintWriter( new PrintWriter( writer ) );

        try
        {
            // When
            log.info( null, new RuntimeException() );
            fail( "Should have thrown " + NullPointerException.class );
        }
        catch ( NullPointerException npe )
        {
            // Then
            assertEquals( "", writer.toString() );
        }
    }

    @Test
    public void loggerWriteLogShouldWorkForNotNullMessageAndNullThrowable()
    {
        // Given
        String message = "I need your clothes, your boots and your motorcycle";
        Throwable throwable = null;

        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( message, throwable );

        // Then
        assertEquals( "1984-10-26 04:23:24.343 INFO  [test] I need your clothes, your boots and your motorcycle\n", writer.toString() );
    }

    @Test
    public void loggerWriteLogShouldWorkForNotNullMessageAndThrowableWithNullMessage()
    {
        // Given
        String message = "The more contact I have with humans, the more I learn";
        Throwable throwable = new Throwable() {
            @Override
            public void printStackTrace( PrintWriter s )
            {
                s.append( "<stacktrace>" );
            }
        };

        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        // When
        log.info( message, throwable );

        // Then
        assertEquals( "1984-10-26 04:23:24.343 INFO  [test] The more contact I have with humans, the more I learn\n<stacktrace>", writer.toString() );
    }

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForNullMessageFormatAndNotNullArguments()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        try
        {
            // When
            log.info( null, "argument1", "argument2" );
            fail( "Should have thrown " + NullPointerException.class );
        }
        catch ( NullPointerException npe )
        {
            // Then
            assertEquals( "", writer.toString() );
        }
    }

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForMessageFormatAndInvalidArguments()
    {
        // Given
        StringWriter writer = new StringWriter();
        Log log = newFormattedLog( writer );

        try
        {
            // When
            log.info( "i expect string and double here [%s, %d] ", "foo", new Object() );
            fail( "Should have thrown " + IllegalFormatException.class );
        }
        catch ( IllegalFormatException ife )
        {
            // Then
            assertEquals( "", writer.toString() );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static FormattedLog newFormattedLog( StringWriter writer )
    {
        return new FormattedLog( Suppliers.singleton( FIXED_DATE ), Suppliers.singleton( new PrintWriter( writer ) ), null, "test", true, true );
    }
}
