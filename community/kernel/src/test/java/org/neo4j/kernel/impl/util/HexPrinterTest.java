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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;

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
        HexPrinter printer = new HexPrinter( out, 2 /*line number digits*/, 8 /*bytes per line*/ );

        // WHEN
        for ( byte value = 0; value < 20; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "00 00 01 02 03 04 05 06 07%n" +
                "01 08 09 0A 0B 0C 0D 0E 0F%n" +
                "02 10 11 12 13" ),
                outStream.toString() );
    }

    @Test
    public void shouldMakeClearEightByteBorders() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );
        HexPrinter printer = new HexPrinter( out, 3 /*line number digits*/, 12 /*bytes per line*/ );

        // WHEN
        for ( byte value = 0; value < 30; value++ )
        {
            printer.append( value );
        }

        // THEN
        out.flush();
        assertEquals( format(
                "000 00 01 02 03 04 05 06 07  08 09 0A 0B%n" +
                "001 0C 0D 0E 0F 10 11 12 13  14 15 16 17%n" +
                "002 18 19 1A 1B 1C 1D" ),
                outStream.toString() );
    }
}
