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
import java.util.IllegalFormatException;

import org.neo4j.function.Supplier;
import org.neo4j.logging.FormattedLog.FormattedLogger;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class FormattedLogTest
{
    @Test
    public void loggerWriteLogShouldNotWriteAnythingForNullMessage()
    {
        // Given
        PrintWriter writer = mock( PrintWriter.class );
        FormattedLogger logger = newFormattedLogger();

        try
        {
            // When
            logger.writeLog( writer, null );
            fail( "Should have thrown " + NullPointerException.class );
        }
        catch ( NullPointerException npe )
        {
            // Then
            verifyZeroInteractions( writer );
        }
    }

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForNullMessageAndNotNullThrowable()
    {
        // Given
        PrintWriter writer = mock( PrintWriter.class );
        FormattedLogger logger = newFormattedLogger();

        try
        {
            // When
            logger.writeLog( writer, null, new RuntimeException() );
            fail( "Should have thrown " + NullPointerException.class );
        }
        catch ( NullPointerException npe )
        {
            // Then
            verifyZeroInteractions( writer );
        }
    }

    @Test
    public void loggerWriteLogShouldWorkForNotNullMessageAndNullThrowable()
    {
        // Given
        String message = "I need your clothes, your boots and your motorcycle";
        Throwable throwable = null;

        PrintWriter writer = mock( PrintWriter.class );
        FormattedLogger logger = newFormattedLogger();

        // When
        logger.writeLog( writer, message, throwable );

        // Then
        verify( writer ).write( message );
        verify( writer, never() ).write( (String) null );
    }

    @Test
    public void loggerWriteLogShouldWorkForNotNullMessageAndThrowableWithNullMessage()
    {
        // Given
        String message = "The more contact I have with humans, the more I learn";
        Throwable throwable = mock( Throwable.class );

        PrintWriter writer = mock( PrintWriter.class );
        FormattedLogger logger = newFormattedLogger();

        // When
        logger.writeLog( writer, message, throwable );

        // Then
        verify( writer ).write( message );
        verify( writer, never() ).write( (String) null );
        verify( throwable ).printStackTrace( writer );
    }

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForNullMessageFormatAndNotNullArguments()
    {
        // Given
        PrintWriter writer = mock( PrintWriter.class );
        FormattedLogger logger = newFormattedLogger();

        try
        {
            // When
            logger.writeLog( writer, null, new Object[]{"argument1", "argument2"} );
            fail( "Should have thrown " + NullPointerException.class );
        }
        catch ( NullPointerException npe )
        {
            // Then
            verifyZeroInteractions( writer );
        }
    }

    @Test
    public void loggerWriteLogShouldNotWriteAnythingForMessageFormatAndInvalidArguments()
    {
        // Given
        PrintWriter writer = mock( PrintWriter.class );
        FormattedLogger logger = newFormattedLogger();

        try
        {
            // When
            logger.writeLog( writer, "i expect string and double here [%s, %d] ", new Object[]{"foo", new Object()} );
            fail( "Should have thrown " + IllegalFormatException.class );
        }
        catch ( IllegalFormatException ife )
        {
            // Then
            verifyZeroInteractions( writer );
        }
    }

    @SuppressWarnings( "unchecked" )
    private static FormattedLogger newFormattedLogger()
    {
        return new FormattedLogger( mock( Supplier.class ), new Object(), "test", true );
    }
}
