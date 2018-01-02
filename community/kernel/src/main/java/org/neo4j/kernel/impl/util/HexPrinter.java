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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static java.nio.ByteBuffer.wrap;

/**
 * Prints streams of bytes as hex, printed in columns and rows neatly ordered. For example:
 *  <p>
 *  @ 0x000A: FF B9 E2 5B 95 2B 69 21  CF 01 10 89 1E 05 67 51  0C 91 32 20 40 8A 4B 92  01 8C C7 93 F8 66 58 F0
 *  <br>
 *  @ 0x000B: 39 C8 F1 2B 84 3B AF 8E  C7 50 F7 82 E7 1C DB 20  BF E3 C1 08 68 12 46 72  BA 72 5F 82 13 9A C1 DF
 *  <br>
 *  @ 0x000C: 56 A6 83 85 36 25 00 DA  B4 57 02 FF E6 97 1C 69  F9 16 56 AF 78 C9 0F A4  CD A4 1F A8 08 08 3B 3B
 *  <p>
 * where number of bytes per line, number of bytes per group, byte group separator, length of line number, prefix
 * or suffix of line number can be controlled. If the length of line number is set to a non-positive number,
 * then no line number, prefix, or suffix will be added.
 *
 */
public class HexPrinter
{
    private final PrintStream out;
    private int bytesPerLine;
    private int bytesPerGroup;
    private String groupSeparator;
    private int maxLineNumberDigits;
    private String lineNumberPrefix;
    private String lineNumberSuffix;

    private long currentLineNumber;
    private int bytesOnThisLine;

    private static final int DEFAULT_BYTES_PER_GROUP = 8;
    private static final int DEFAULT_BYTES_PER_LINE = DEFAULT_BYTES_PER_GROUP * 4;
    private static final int DEFAULT_MAX_LINE_NUMBER_DIGITS = 0;
    private static final String DEFAULT_GROUP_SEPARATOR = "    ";
    private static final String DEFAULT_LINE_NUMBER_PREFIX = "@ ";
    private static final String DEFAULT_LINE_NUMBER_SUFFIX = ": ";

    public HexPrinter withBytesPerLine( int bytesPerLine )
    {
        this.bytesPerLine = bytesPerLine;
        return this;
    }

    public HexPrinter withBytesPerGroup( int bytesPerGroup )
    {
        this.bytesPerGroup = bytesPerGroup;
        return this;
    }

    public HexPrinter withGroupSeparator( String separator )
    {
        this.groupSeparator = separator;
        return this;
    }

    public HexPrinter withLineNumberDigits( int maxLineNumberDigits )
    {
        this.maxLineNumberDigits = maxLineNumberDigits;
        return this;
    }

    public HexPrinter withLineNumberPrefix( String prefix )
    {
        this.lineNumberPrefix = prefix;
        return this;
    }

    public HexPrinter withLineNumberSuffix( String suffix )
    {
        this.lineNumberSuffix = suffix;
        return this;
    }

    public HexPrinter withLineNumberOffset( long offset )
    {
        this.currentLineNumber = offset;
        return this;
    }

    public HexPrinter withBytesGroupingFormat( int bytesPerLine, int bytesPerGroup, String separator )
    {
        this.bytesPerLine = bytesPerLine;
        this.bytesPerGroup = bytesPerGroup;
        this.groupSeparator = separator;
        return this;
    }

    public HexPrinter withLineNumberFormat( int maxLineNumberDigits, String prefix, String suffix )
    {
        this.maxLineNumberDigits = maxLineNumberDigits;
        this.lineNumberPrefix = prefix;
        this.lineNumberSuffix = suffix;
        return this;
    }

    /**
     * Using no line number, 8 bytes per group, 32 bytes per line, 4-space separator as default formating to
     * print bytes as hex. Output looks like:
     * <p>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * <br>
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param out
     */
    public HexPrinter( PrintStream out )
    {
        this.out = out;
        this.bytesPerLine = DEFAULT_BYTES_PER_LINE;
        this.bytesPerGroup = DEFAULT_BYTES_PER_GROUP;
        this.groupSeparator = DEFAULT_GROUP_SEPARATOR;
        this.maxLineNumberDigits = DEFAULT_MAX_LINE_NUMBER_DIGITS;
        this.lineNumberPrefix = DEFAULT_LINE_NUMBER_PREFIX;
        this.lineNumberSuffix = DEFAULT_LINE_NUMBER_SUFFIX;
    }

    /**
     * Append one byte into the print stream
     * @param value
     * @return
     */
    public HexPrinter append( byte value )
    {
        checkNewLine();
        addHexValue( value );
        return this;
    }

    /**
     * Append all the bytes in the channel into print stream
     * @param source
     * @return
     * @throws IOException
     */
    public HexPrinter append( ReadableByteChannel source ) throws IOException
    {
        return append( source, -1 );
    }

    /**
     * Append {@code atMost} count of bytes into print stream
     * @param source
     * @param atMost
     * @return
     * @throws IOException
     */
    public HexPrinter append( ReadableByteChannel source, int atMost ) throws IOException
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
        return this;
    }

    /**
     * Append a part of byte buffer into print stream
     * @param bytes
     * @param offset
     * @param length
     * @return
     */
    public HexPrinter append( ByteBuffer bytes, int offset, int length )
    {
        for ( int i = offset; i < offset + length; i++ )
        {
            append( bytes.get( i ) );
        }
        return this;
    }

    /**
     * Append the bytes in the byte buffer, from its current position to its limit into print stream. This operation
     * will not move the buffers current position.
     * @param bytes
     * @return
     */
    public HexPrinter append( ByteBuffer bytes )
    {
        return append( bytes, bytes.position(), bytes.remaining() );
    }

    /**
     * Append the whole byte array into print stream
     * @param bytes
     * @return
     */
    public HexPrinter append( byte[] bytes )
    {
        return append( wrap( bytes ), 0, bytes.length );
    }

    private void addHexValue( byte value )
    {
        if ( bytesOnThisLine == 1 )
        {
            // it is the first byte
            // out.append( NOTHING )
        }
        else if ( bytesOnThisLine % bytesPerGroup == 1 )
        {
            // it is the first byte for a new byte group
            out.append( groupSeparator );
        }
        else
        {
            out.append( " " );
        }
        out.printf( "%X%X", 0xF & (value >> 4), 0xF & value );
    }

    private void checkNewLine()
    {
        if ( bytesOnThisLine >= bytesPerLine )
        {
            out.println();
            bytesOnThisLine = 0;
            currentLineNumber++;
        }
        if ( bytesOnThisLine == 0 && maxLineNumberDigits > 0 )
        {
            // a new line and line number enabled
            out.append( lineNumberPrefix );
            out.printf( "0x%0" + maxLineNumberDigits + "X", currentLineNumber );
            out.append( lineNumberSuffix );
        }
        bytesOnThisLine++;
    }

    // Some static methods that could be used directly

    /**
     * Convert a subsection of a byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @param offset
     * @param length
     * @return formatted hex numbers in string
     */
    public static String hex( ByteBuffer bytes, int offset, int length )
    {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        PrintStream out = new PrintStream( outStream );

        new HexPrinter( out ).append( bytes, offset, length );
        out.flush();
        return outStream.toString();
    }

    /**
     * Convert a full byte buffer to a human readable string of nicely formatted hex numbers using default hex format.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @return formatted hex numbers in string
     */
    public static String hex( ByteBuffer bytes )
    {
        return hex( bytes, bytes.position(), bytes.limit() );
    }

    /**
     * Convert a full byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @return formatted hex numbers in string
     */
    public static String hex( byte[] bytes )
    {
        return hex( wrap( bytes ) );
    }

    /**
     * Convert a single byte to a human-readable hex number. The number will always be two characters wide.
     * @param b
     * @return formatted hex numbers in string
     */
    public static String hex( byte b )
    {
        return String.format( "%02X", b );
    }
}
