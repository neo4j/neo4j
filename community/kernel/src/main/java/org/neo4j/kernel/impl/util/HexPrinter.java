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

import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static java.lang.Math.max;
import static java.lang.String.format;
import static java.lang.String.valueOf;

/**
 * Prints streams of bytes as hex, printed in columns and rows neatly ordered. For example:
 *
 *  0000 FF B9 E2 5B 95 2B 69 21  CF 01 10 89 1E 05 67 51  0C 91 32 20 40 8A 4B 92  01 8C C7 93 F8 66 58 F0
 *  0001 39 C8 F1 2B 84 3B AF 8E  C7 50 F7 82 E7 1C DB 20  BF E3 C1 08 68 12 46 72  BA 72 5F 82 13 9A C1 DF
 *  0002 56 A6 83 85 36 25 00 DA  B4 57 02 FF E6 97 1C 69  F9 16 56 AF 78 C9 0F A4  CD A4 1F A8 08 08 3B 3B
 *
 * where number of bytes per line and can be controlled.
 *
 * @author Mattias Persson
 */
public class HexPrinter
{
    private final PrintStream out;
    private final int bytesPerLine;
    private final int maxLineNumberDigits;
    private int currentLineNumber;
    private int bytesOnThisLine;

    public HexPrinter( PrintStream out )
    {
        this( out, 8, 8 );
    }

    public HexPrinter( PrintStream out, int maxLineNumberDigits, int bytesPerLine )
    {
        this.out = out;
        this.maxLineNumberDigits = maxLineNumberDigits;
        this.bytesPerLine = bytesPerLine;
    }

    public HexPrinter append( byte value )
    {
        checkNewLine();
        addHexValue( value );
        return this;
    }

    public void append( ReadableByteChannel source ) throws IOException
    {
        append( source, -1 );
    }

    public void append( ReadableByteChannel source, int atMost ) throws IOException
    {
        boolean indefinite = atMost == -1;
        ByteBuffer buffer = ByteBuffer.allocate( 4*1024 );
        while ( true )
        {
            buffer.clear();
            if ( !indefinite )
            {
                buffer.limit( Math.min( buffer.capacity(), atMost ) );
            }
            int read = source.read( buffer );
            if ( read == -1 )
            {
                break;
            }

            atMost -= read;
            buffer.flip();
            while ( buffer.hasRemaining() )
            {
                append( buffer.get() );
            }
        }
    }

    private void addHexValue( byte value )
    {
        out.append( " " )
           .append( (bytesOnThisLine > 1 && bytesOnThisLine%8 == 1 ) ? " " : "" )
           .append( format( "%X", ((value&0xF0)>>4) ) )
           .append( format( "%X", value&0xF ) );
    }

    private void addLineNumber()
    {
        String toAppend = valueOf( currentLineNumber );
        int zerosToPad = max( 0, maxLineNumberDigits-toAppend.length() );
        while ( zerosToPad --> 0 )
        {
            out.append( '0' );
        }
        out.append( toAppend );
    }

    private void checkNewLine()
    {
        if ( bytesOnThisLine >= bytesPerLine )
        {
            out.println();
            bytesOnThisLine = 0;
            currentLineNumber++;
        }
        if ( bytesOnThisLine == 0 )
        {
            addLineNumber();
        }
        bytesOnThisLine++;
    }
}
