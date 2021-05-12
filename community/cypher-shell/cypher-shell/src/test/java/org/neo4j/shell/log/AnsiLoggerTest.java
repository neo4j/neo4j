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
package org.neo4j.shell.log;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.PrintStream;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.CommandException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class AnsiLoggerTest
{
    private final PrintStream out = mock( PrintStream.class );
    private final PrintStream err = mock( PrintStream.class );
    private AnsiLogger logger = new AnsiLogger( false, Format.VERBOSE, out, err );

    @Test
    void defaultStreams()
    {
        Logger logger = new AnsiLogger( false );

        assertEquals( System.out, logger.getOutputStream() );
        assertEquals( System.err, logger.getErrorStream() );
    }

    @Test
    void customStreams()
    {
        assertEquals( out, logger.getOutputStream() );
        assertEquals( err, logger.getErrorStream() );
    }

    @Test
    void printError()
    {
        logger.printError( "bob" );
        verify( err ).println( "bob" );
    }

    @Test
    void printException()
    {
        logger.printError( new Throwable( "bam" ) );
        verify( err ).println( "bam" );
    }

    @Test
    void printExceptionWithDebug()
    {
        Logger logger = new AnsiLogger( true, Format.VERBOSE, out, err );
        logger.printError( new Throwable( "bam" ) );
        verify( err ).println( contains( "java.lang.Throwable: bam" ) );
        verify( err ).println( contains( "at org.neo4j.shell.log.AnsiLoggerTest.printExceptionWithDebug" ) );
    }

    @Test
    void printOut()
    {
        logger.printOut( "sob" );
        verify( out ).println( "sob" );
    }

    @Test
    void printOutManyShouldNotBuildState()
    {
        logger.printOut( "bob" );
        logger.printOut( "nob" );
        logger.printOut( "cod" );

        verify( out ).println( "bob" );
        verify( out ).println( "nob" );
        verify( out ).println( "cod" );
    }

    @Test
    void printErrManyShouldNotBuildState()
    {
        logger.printError( "bob" );
        logger.printError( "nob" );
        logger.printError( "cod" );

        verify( err ).println( "bob" );
        verify( err ).println( "nob" );
        verify( err ).println( "cod" );
    }

    @Test
    void printIfVerbose()
    {
        logger = new AnsiLogger( false, Format.VERBOSE, out, err );

        logger.printIfDebug( "deb" );
        logger.printIfVerbose( "foo" );
        logger.printIfPlain( "bar" );

        verify( out ).println( "foo" );
        verifyNoMoreInteractions( out );
    }

    @Test
    void printIfPlain()
    {
        logger = new AnsiLogger( false, Format.PLAIN, out, err );

        logger.printIfDebug( "deb" );
        logger.printIfVerbose( "foo" );
        logger.printIfPlain( "bar" );

        verify( out ).println( "bar" );
        verifyNoMoreInteractions( out );
    }

    @Test
    void printIfDebug()
    {
        logger = new AnsiLogger( true, Format.PLAIN, out, err );

        logger.printIfDebug( "deb" );
        logger.printIfVerbose( "foo" );
        logger.printIfPlain( "bar" );

        verify( out ).println( "deb" );
        verify( out ).println( "bar" );
        verifyNoMoreInteractions( out );
    }

    @Test
    void testSimple()
    {
        assertEquals( "@|RED yahoo|@", logger.getFormattedMessage( new NullPointerException( "yahoo" ) ) );
    }

    @Test
    void testNested()
    {
        assertEquals( "@|RED outer|@", logger.getFormattedMessage( new ClientException( "outer",
                                                                                        new CommandException( "nested" ) ) ) );
    }

    @Test
    void testNestedDeep()
    {
        assertEquals( "@|RED outer|@", logger.getFormattedMessage(
                new ClientException( "outer",
                                     new ClientException( "nested",
                                                          new ClientException( "nested deep" ) ) ) ) );
    }

    @Test
    void testNullMessage()
    {
        assertEquals( "@|RED ClientException|@", logger.getFormattedMessage( new ClientException( null ) ) );
        assertEquals( "@|RED outer|@",
                      logger.getFormattedMessage( new ClientException( "outer", new NullPointerException( null ) ) ) );
    }

    @Test
    void testExceptionGetsFormattedMessage()
    {
        AnsiLogger logger = spy( this.logger );
        logger.printError( new NullPointerException( "yahoo" ) );
        verify( logger ).printError( "@|RED yahoo|@" );
    }
}
