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
package org.neo4j.helpers;

import java.io.IOException;
import java.util.Arrays;

/**
 * Helper functions for working with strings.
 */
public final class Strings
{

    public static final String TAB = "\t";

    private Strings()
    {
    }

    /**
     * @deprecated This field will be removed in the next major release.
     */
    @Deprecated
    public static final Function<String,String> decamelize = new Function<String,String>()
    {
        @Override
        public String apply( String name )
        {
            StringBuilder result = new StringBuilder();
            for ( int i = 0; i < name.length(); i++ )
            {
                char c = name.charAt( i );
                if ( Character.isUpperCase( c ) )
                {
                    if ( i > 0 )
                    {
                        result.append( '_' );
                    }
                    result.append( Character.toLowerCase( c ) );
                }
                else
                {
                    result.append( c );
                }
            }
            return result.toString();
        }
    };

    /**
     * @deprecated This method will be removed in the next major release.
     */
    @Deprecated
    public static boolean isBlank( String str )
    {
        if ( str == null || str.isEmpty() )
        {
            return true;
        }
        for ( int i = 0; i < str.length(); i++ )
        {
            if ( !Character.isWhitespace( str.charAt( i ) ) )
            {
                return false;
            }
        }
        return true;
    }

    /**
     * @deprecated This method will be removed in the next major release.
     */
    @Deprecated
    public static String defaultIfBlank( String str, String defaultStr )
    {
        return isBlank( str ) ? defaultStr : str;
    }

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

    public static String escape( String arg )
    {
        StringBuilder builder = new StringBuilder( arg.length() );
        try
        {
            escape( builder, arg );
        }
        catch ( IOException e )
        {
            throw new ThisShouldNotHappenError( "Stefan", "IOException from using StringBuilder", e );
        }
        return builder.toString();
    }

    /**
     * Joining independent lines from provided elements into one line with {@link java.lang.System#lineSeparator} after
     * each element
     * @param elements - lines to join
     * @return joined line
     */
    public static String joinAsLines( String... elements )
    {
        StringBuilder result = new StringBuilder();
        for ( String line : elements )
        {
            result.append( line ).append( System.lineSeparator() );
        }
        return result.toString();
    }

    public static void escape( Appendable output, String arg ) throws IOException
    {
        int len = arg.length();
        for ( int i = 0; i < len; i++ )
        {
            char ch = arg.charAt( i );
            switch (ch) {
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

    /**
     * Use this to standardize the width of some text output to all be left-justified and space-padded
     * on the right side to fill up the given column width.
     *
     * @param str the text to format
     * @param columnWidth the column width
     * @return the left-justified space-padded text
     * @deprecated This method will be removed in the next major release.
     */
    @Deprecated
    public static String ljust( String str, int columnWidth )
    {
        return String.format( "%-" + columnWidth + "s", str );
    }

    /**
     * Use this to standardize the width of some text output to all be right-justified and space-padded
     * on the left side to fill up the given column width.
     *
     * @param str the text to format
     * @param columnWidth the column width
     * @return the right-justified space-padded text
     * @deprecated This method will be removed in the next major release.
     */
    @Deprecated
    public static String rjust( String str, int columnWidth )
    {
        return String.format( "%" + columnWidth + "s", str );
    }
}
