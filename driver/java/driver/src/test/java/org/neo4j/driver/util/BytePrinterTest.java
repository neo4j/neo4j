/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver.util;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class BytePrinterTest
{
    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Test
    public void shouldPrintBytes() throws Throwable
    {
        assertEquals( "01", BytePrinter.hex( (byte) 1 ) );
        assertEquals( "01 02 03 ", BytePrinter.hex( new byte[]{1, 2, 3} ) );
        assertEquals( "hello     ", BytePrinter.ljust( "hello", 10 ) );
        assertEquals( "     hello", BytePrinter.rjust( "hello", 10 ) );

        BytePrinter.print( (byte) 1, new PrintStream( baos ) );
        assertEquals( "01", new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );

        baos.reset();
        BytePrinter.print( new byte[]{1, 2, 3}, new PrintStream( baos ) );
        assertEquals( "01 02 03 ", new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );

        baos.reset();
        BytePrinter.print( ByteBuffer.wrap( new byte[]{1, 2, 3} ), new PrintStream( baos ) );
        assertEquals( "01 02 03 ", new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );
    }
}