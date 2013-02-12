/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Utility to convert and print binary data in a human readable way.
 */
public class BytePrinter
{

    /**
     * Print a full byte buffer as nicely formatted groups of hex numbers.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @param out
     */
    public static void print( ByteBuffer bytes, PrintStream out )
    {
        print( bytes, out, 0, bytes.capacity() );
    }

    /**
     * Print a subsection of a byte buffer as nicely formatted groups of hex numbers.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @param out
     */
    public static void print( ByteBuffer bytes, PrintStream out, int offset, int length )
    {
        for(int i=offset;i<offset + length;i++)
        {
            print( bytes.get( i ), out );
            if((i - offset + 1) % 32 == 0)
            {
                out.println(  );
            } else if((i - offset + 1) % 8 == 0)
            {
                out.print( "    " );
            } else {
                out.print( " " );
            }
        }
    }

    /**
     * Print a single byte as a hex number. The number will always be two characters wide.
     *
     * @param b
     * @param out
     */
    public static void print( byte b, PrintStream out )
    {
        out.print( hex( b ) );
    }

    /**
     * This should not be in this class, move to a dedicated ascii-art class when appropriate.
     *
     * Use this to standardize the width of some text output to all be left-justified and space-padded
     * on the right side to fill up the given column width.
     *
     * @param str
     * @param columnWidth
     * @return
     */
    public static String ljust( String str, int columnWidth )
    {
        return String.format( "%-" + columnWidth + "s", str);
    }

    /**
     * This should not be in this class, move to a dedicated ascii-art class when appropriate.
     *
     * Use this to standardize the width of some text output to all be right-justified and space-padded
     * on the left side to fill up the given column width.
     *
     * @param str
     * @param columnWidth
     * @return
     */
    public static String rjust( String str, int columnWidth )
    {
        return String.format( "%" + columnWidth + "s", str);
    }

    /**
     * Convert a single byte to a human-readable hex number. The number will always be two characters wide.
     * @param b
     * @return
     */
    public static String hex(byte b)
    {
        return String.format("%02x", b);
    }

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
     * @return
     */
    public static String hex( ByteBuffer bytes, int offset, int length )
    {
        try
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = null;
            ps = new PrintStream(baos, true, "UTF-8");
            print( bytes, ps, offset, length );
            return baos.toString("UTF-8");
        }
        catch ( UnsupportedEncodingException e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * Convert a full byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @return
     */
    public static String hex(ByteBuffer bytes)
    {
        return hex( bytes, 0, bytes.capacity() );
    }

    /**
     * Convert a full byte buffer to a human readable string of nicely formatted hex numbers.
     * Output looks like:
     *
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     * 01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08    01 02 03 04 05 06 07 08
     *
     * @param bytes
     * @return
     */
    public static String hex(byte[] bytes)
    {
        return hex( ByteBuffer.wrap( bytes ) );
    }

}
