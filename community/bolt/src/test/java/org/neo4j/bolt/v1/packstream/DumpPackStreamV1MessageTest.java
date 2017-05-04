/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.packstream;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.List;

import org.neo4j.bolt.v1.messaging.message.Message;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class DumpPackStreamV1MessageTest
{
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldDechunk() throws Throwable
    {
        // Given
        String input = "00 0F B1 01 8C 4D 79 43    6C 69 65 6E 74 2F 31 2E    30 00 00";
        input = input.replaceAll( "\\s+", "" );
        input += input; // same message twice
        byte[] bytes = DumpPackStreamV1Message.hexStringToBytes( input );

        // When
        List<Message> messages = DumpPackStreamV1Message.dechunk( bytes );

        // Then
        assertThat( messages.toString(),
                equalTo( "[InitMessage{clientName='MyClient/1.0'}, InitMessage{clientName='MyClient/1.0'}]" ) );
    }

    @Test
    public void shouldDechunkWhenMessageInTwoChunks() throws Throwable
    {
        // Given
        String input = "00 08 B1 01 8C 4D 79 43 6C 69 00 07 65 6E 74 2F 31 2E 30 00 00";
        input = input.replaceAll( "\\s+", "" );
        input += input; // same message twice
        byte[] bytes = DumpPackStreamV1Message.hexStringToBytes( input );

        // When
        List<Message> messages = DumpPackStreamV1Message.dechunk( bytes );

        // Then
        assertThat( messages.toString(),
                equalTo( "[InitMessage{clientName='MyClient/1.0'}, InitMessage{clientName='MyClient/1.0'}]" ) );
    }

    @Test
    public void shouldIndicateErrorAtCorrectPalceWhenMessageInTwoChunks() throws Throwable
    {
        // Given
        String input = "00 08 B1 01 8C 4D 79 43 6C 69 00 07 65 AA 74 2F 31 2E 30 00 00";
        input = input.replaceAll( "\\s+", "" );
        byte[] bytes = DumpPackStreamV1Message.hexStringToBytes( input );

        // When & Then
        List<Message> messages = DumpPackStreamV1Message.dechunk( bytes );

        exception.expect( IOException.class );
        exception.expectMessage( String.format( "00%n      ^^" ) );
        DumpPackStreamV1Message.dechunk( bytes );
    }


    @Test
    public void shouldPrintHelpfulMessageWhenDechunkFailed() throws Throwable
    {
        // Given
        String input = "00 0F CC 01 8C 4D 79 43    6C 69 65 6E 74 2F 31 2E    30 00 00";
        input = input.replaceAll( "\\s+", "" );
        byte[] bytes = DumpPackStreamV1Message.hexStringToBytes( input );

        // When & Then
        exception.expect( IOException.class );
        exception.expectMessage( String.format( "00%n      ^^" ) );
        DumpPackStreamV1Message.dechunk( bytes );
    }

    @Test
    public void shouldUnpack() throws Throwable
    {
        // Given
        String input = "B1 01 8C 4D 79 43    6C 69 65 6E 74 2F 31 2E    30";
        input = input.replaceAll( "\\s+", "" );
        input += input; // same message twice
        byte[] bytes = DumpPackStreamV1Message.hexStringToBytes( input );

        // When
        List<Message> messages = DumpPackStreamV1Message.unpack( bytes );

        // Then
        assertThat( messages.toString(),
                equalTo( "[InitMessage{clientName='MyClient/1.0'}, InitMessage{clientName='MyClient/1.0'}]" ) );
    }


    @Test
    public void shouldPrintHelpfulMessageWhenUnpackFailed() throws Throwable
    {
        // Given
        String input = "B1 01 8C 4D 79 43    6C 69 65 6E 74 2F 31 2E    30 00 00";
        input = input.replaceAll( "\\s+", "" );
        byte[] bytes = DumpPackStreamV1Message.hexStringToBytes( input );

        // When & Then
        exception.expect( IOException.class );
        exception.expectMessage( String.format( "00%n                                                ^^" ) );
        DumpPackStreamV1Message.unpack( bytes );
    }
}
