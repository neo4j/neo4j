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
package org.neo4j.helpers;

import java.io.IOException;
import java.util.Arrays;

/**
 * Helper functions for working with strings.
 */
@Deprecated
public final class Strings
{
    @Deprecated
    public static final String TAB = "\t";

    private Strings()
    {
    }

    @Deprecated
    public static String prettyPrint( Object o )
    {
        if ( o == null )
        {
            return "null";
        }

        Class<?> clazz = o.getClass();
        if ( clazz.isArray() )
        {
            if ( clazz == byte[].class )
            {
                return Arrays.toString( (byte[]) o );
            }
            else if ( clazz == short[].class )
            {
                return Arrays.toString( (short[]) o );
            }
            else if ( clazz == int[].class )
            {
                return Arrays.toString( (int[]) o );
            }
            else if ( clazz == long[].class )
            {
                return Arrays.toString( (long[]) o );
            }
            else if ( clazz == float[].class )
            {
                return Arrays.toString( (float[]) o );
            }
            else if ( clazz == double[].class )
            {
                return Arrays.toString( (double[]) o );
            }
            else if ( clazz == char[].class )
            {
                return Arrays.toString( (char[]) o );
            }
            else if ( clazz == boolean[].class )
            {
                return Arrays.toString( (boolean[]) o );
            }
            else
            {
                return Arrays.deepToString( (Object[]) o );
            }
        }
        else
        {
            return String.valueOf( o );
        }
    }

    @Deprecated
    public static String escape( String arg )
    {
        StringBuilder builder = new StringBuilder( arg.length() );
        try
        {
            escape( builder, arg );
        }
        catch ( IOException e )
        {
            throw new AssertionError( "IOException from using StringBuilder", e );
        }
        return builder.toString();
    }

    /**
     * Joining independent lines from provided elements into one line with {@link java.lang.System#lineSeparator} after
     * each element
     * @param elements - lines to join
     * @return joined line
     */
    @Deprecated
    public static String joinAsLines( String... elements )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : elements )
        {
            result.append( line ).append( System.lineSeparator() );
        }
        return result.toString();
    }

    @Deprecated
    public static void escape( Appendable output, String arg ) throws IOException
    {
        int len = arg.length();
        for ( int i = 0; i < len; i++ )
        {
            char ch = arg.charAt( i );
            switch ( ch )
            {
                case '"':
                    output.append( "\\\"" );
                    break;

                case '\'':
                    output.append( "\\\'" );
                    break;

                case '\\':
                    output.append( "\\\\" );
                    break;

                case '\n':
                    output.append( "\\n" );
                    break;

                case '\t':
                    output.append( "\\t" );
                    break;

                case '\r':
                    output.append( "\\r" );
                    break;

                case '\b':
                    output.append( "\\b" );
                    break;

                case '\f':
                    output.append( "\\f" );
                    break;

                default:
                    output.append( ch );
                    break;
            }
        }
    }
}
