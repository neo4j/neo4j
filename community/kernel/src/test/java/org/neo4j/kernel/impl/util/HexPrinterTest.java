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

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class HexPrinterTest
{
    @Test
    public void shouldPrintACoupleOfLines() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        HexPrinter printer = new HexPrinter( out );

        // WHEN
        for ( byte value = 0; value < 40; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "00 01 02 03 04 05 06 07    08 09 0A 0B 0C 0D 0E 0F    " +
                "10 11 12 13 14 15 16 17    18 19 1A 1B 1C 1D 1E 1F%n" +
                "20 21 22 23 24 25 26 27" ),
                outStream.toString() );
    }

    @Test
    public void shouldPrintUserSpecifiedBytesGroupingFormat() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        HexPrinter printer = new HexPrinter( out ).withBytesGroupingFormat( 12, 4, ", " );

        // WHEN
        for ( byte value = 0; value < 30; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "00 01 02 03, 04 05 06 07, 08 09 0A 0B%n" +
                "0C 0D 0E 0F, 10 11 12 13, 14 15 16 17%n" +
                "18 19 1A 1B, 1C 1D" ),
                outStream.toString() );
    }

    @Test
    public void shouldNotGroupingWhenBytesPerGroupIsGreaterThanBytesPerLine() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        HexPrinter printer = new HexPrinter( out ).withBytesPerLine( 12 ).withBytesPerGroup( 100 );

        // WHEN
        for ( byte value = 0; value < 30; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "00 01 02 03 04 05 06 07 08 09 0A 0B%n" +
                "0C 0D 0E 0F 10 11 12 13 14 15 16 17%n" +
                "18 19 1A 1B 1C 1D" ),
                outStream.toString() );
    }

    @Test
    public void shouldPrintUserSpecifiedLineNumberFormat() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        HexPrinter printer = new HexPrinter( out ).withLineNumberFormat( 5, "[", "]" );

        // WHEN
        for ( byte value = 0; value < 40; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "[0x00000]" +
                "00 01 02 03 04 05 06 07    08 09 0A 0B 0C 0D 0E 0F    " +
                "10 11 12 13 14 15 16 17    18 19 1A 1B 1C 1D 1E 1F%n" +
                "[0x00001]" +
                "20 21 22 23 24 25 26 27" ),
                outStream.toString() );
    }

    @Test
    public void shouldStartFromUserSpecifiedLineNumber() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        HexPrinter printer = new HexPrinter( out ).withLineNumberDigits( 2 ).withLineNumberOffset( 0xA8 );

        // WHEN
        for ( byte value = 0; value < 40; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "@ 0xA8: " +
                "00 01 02 03 04 05 06 07    08 09 0A 0B 0C 0D 0E 0F    " +
                "10 11 12 13 14 15 16 17    18 19 1A 1B 1C 1D 1E 1F%n" +
                "@ 0xA9: " +
                "20 21 22 23 24 25 26 27"  ),
                outStream.toString() );
    }

    @Test
    public void shouldPrintPartOfByteBuffer() throws Exception
    {
        ByteBuffer bytes = ByteBuffer.allocate( 1024 );
        for ( byte value = 0; value < 33; value++ )
        {
            bytes.put( value );
        }
        String hexString = HexPrinter.hex( bytes, 3, 8 );
        assertEquals( format( "03 04 05 06 07 08 09 0A" ), hexString );
    }

    @Test
    public void shouldOnlyPrintBytesWrittenToBuffer() throws Exception
    {
        // Given
        ByteBuffer bytes = ByteBuffer.allocate( 1024 );
        for ( byte value = 0; value < 10; value++ )
        {
            bytes.put( value );
        }
        bytes.flip();

        // When
        String hexString = HexPrinter.hex( bytes );

        // Then
        assertEquals( format( "00 01 02 03 04 05 06 07    08 09" ), hexString );
    }
}
