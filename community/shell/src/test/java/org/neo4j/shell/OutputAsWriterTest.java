/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.shell;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.shell.impl.SystemOutput;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.SuppressOutput;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isEmptyString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith( SuppressOutputExtension.class )
class OutputAsWriterTest
{

    @Inject
    private SuppressOutput suppressOutput;
    private OutputAsWriter writer;

    @BeforeEach
    void setUp()
    {
        writer = new OutputAsWriter( new SystemOutput() );
    }

    @Test
    void shouldNotFlushWithoutNewline() throws Exception
    {
        writer.write( "foo".toCharArray() );
        assertThat( suppressOutput.getOutputVoice().toString(), isEmptyString() );
    }

    @Test
    void shouldFlushWithNewline() throws Exception
    {
        SuppressOutput.Voice outputVoice = suppressOutput.getOutputVoice();
        String s = format( "foobar%n" );
        writer.write( s.toCharArray() );
        assertEquals( s.length(), outpuLengthInBytes( outputVoice ) );
        assertEquals( s, outputVoice.toString() );
    }

    @Test
    void shouldFlushPartiallyWithNewlineInMiddle() throws Exception
    {
        SuppressOutput.Voice outputVoice = suppressOutput.getOutputVoice();
        String firstPart = format( "foo%n" );
        String secondPart = "bar";
        String string = firstPart + secondPart;
        String newLine = format( "%n" );
        String fullString = string + newLine;

        writer.write( string.toCharArray() );
        String firstLine = outputVoice.lines().get( 0 );
        assertNotNull( firstLine );
        assertEquals( firstPart.length(), outpuLengthInBytes( outputVoice ) );
        assertEquals( "foo", firstLine );

        writer.write( newLine.toCharArray() );
        String secondLine = outputVoice.lines().get( 1 );
        assertNotNull( secondLine );
        assertEquals( fullString.length(), outpuLengthInBytes( outputVoice ) );
        assertEquals( fullString, outputVoice.toString() );

    }

    private static int outpuLengthInBytes( SuppressOutput.Voice outputVoice )
    {
        return outputVoice.toString().getBytes().length;
    }
}
