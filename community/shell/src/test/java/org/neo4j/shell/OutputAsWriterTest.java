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
package org.neo4j.shell;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.shell.impl.SystemOutput;

public class OutputAsWriterTest
{

    private PrintStream out;
    private ByteArrayOutputStream buffer;
    private SystemOutput output;
    private OutputAsWriter writer;

    @Before
    public void setUp() throws Exception
    {
        out = System.out;
        buffer = new ByteArrayOutputStream();
        System.setOut( new PrintStream( buffer ) );
        output = new SystemOutput();
        writer = new OutputAsWriter( output );
    }

    @After
    public void tearDown() throws Exception
    {
        System.setOut( out );
    }

    @Test
    public void shouldNotFlushWithoutNewline() throws Exception
    {
        writer.write( "foo".toCharArray() );
        assertEquals( 0, buffer.size() );
    }

    @Test
    public void shouldFlushWithNewline() throws Exception
    {
        String s = format( "foobar%n" );
        writer.write( s.toCharArray() );
        assertEquals( s.length(), buffer.size() );
        assertEquals( s, buffer.toString() );
    }

    @Test
    public void shouldFlushPartiallyWithNewlineInMiddle() throws Exception
    {
        String firstPart = format( "foo%n" );
        String secondPart = "bar";
        String string = firstPart + secondPart;
        String newLine = format( "%n" );
        String fullString = string + newLine;

        writer.write( string.toCharArray() );
        assertEquals( firstPart.length(), buffer.size() );
        assertEquals( firstPart, buffer.toString() );

        writer.write( newLine.toCharArray() );
        assertEquals( fullString.length(), buffer.size() );
        assertEquals( fullString, buffer.toString() );

    }
}
