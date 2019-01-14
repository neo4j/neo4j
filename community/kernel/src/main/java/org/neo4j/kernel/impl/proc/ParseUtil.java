/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.proc;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Contains simple parsing utils for parsing Cypher lists, maps and values.
 *
 * The methods here are not very optimized and are deliberately simple. If you find yourself using these method
 * for parsing big json documents you should probably rethink your choice.
 */
public final class ParseUtil
{
    private ParseUtil()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    static Map<String,Object> parseMap( String s )
    {
        int pos = 0;
        int braceCounter = 0;
        Map<String,Object> map = new HashMap<>();
        StringBuilder builder = new StringBuilder();
        boolean inList = false;
        while ( pos < s.length() )
        {

            char character = s.charAt( pos );
            switch ( character )
            {
            case ' ':
                ++pos;
                break;
            case '{':
                if ( braceCounter++ > 0 )
                {
                    builder.append( s.charAt( pos ) );
                }
                ++pos;
                break;
            case ',':
                if ( !inList && braceCounter == 1 )
                {
                    addKeyValue( map, builder.toString().trim() );
                    builder = new StringBuilder();
                }
                else
                {
                    builder.append( s.charAt( pos ) );
                }
                ++pos;
                break;
            case '}':
                if ( --braceCounter == 0 )
                {
                    addKeyValue( map, builder.toString().trim() );
                }
                else
                {
                    builder.append( s.charAt( pos ) );
                }
                ++pos;
                break;
            case '[':
                inList = true;
                builder.append( s.charAt( pos++ ) );
                break;
            case ']':
                inList = false;
                builder.append( s.charAt( pos++ ) );
                break;
            default:
                builder.append( s.charAt( pos++ ) );
                break;
            }
        }
        if ( braceCounter != 0 )
        {
            throw new IllegalArgumentException( String.format( "%s contains unbalanced '{', '}'.", s ) );
        }

        return map;
    }

    private static void addKeyValue( Map<String,Object> map, String keyValue )
    {
        if ( keyValue.isEmpty() )
        {
            return;
        }
        int split = keyValue.indexOf( ':' );
        if ( split < 0 )
        {
            throw new IllegalArgumentException( "Keys and values must be separated with ':'" );
        }
        String key = parseKey( keyValue.substring( 0, split ).trim() );
        Object value = parseValue( keyValue.substring( split + 1 ).trim(), Object.class );

        if ( map.containsKey( key ) )
        {
            throw new IllegalArgumentException(
                    String.format( "Multiple occurrences of key '%s'", key ) );
        }
        map.put( key, value );
    }

    private static String parseKey( String s )
    {
        int pos = 0;
        while ( pos < s.length() )
        {
            char c = s.charAt( pos );
            switch ( c )
            {
            case '\'':
            case '\"':
                ++pos;
                break;
            default:
                return s.substring( pos, s.length() - pos );
            }
        }

        throw new IllegalArgumentException( "" );
    }

    /**
     * Parses value into object. Make sure you call trim on the string
     * before calling this method. The type is used for type checking lists.
     */
    private static Object parseValue( String s, Type type )
    {
        int pos = 0;
        while ( pos < s.length() )
        {
            char c = s.charAt( pos );
            int closing;
            switch ( c )
            {
            case ' ':
                ++pos;
                break;
            case '\'':
                closing = s.lastIndexOf( '\'' );
                if ( closing < 0 )
                {
                    throw new IllegalArgumentException( "Did not find a matching end quote, '" );
                }
                return s.substring( pos + 1, closing );
            case '\"':
                closing = s.lastIndexOf( '\"' );
                if ( closing < 0 )
                {
                    throw new IllegalArgumentException( "Did not find a matching end quote, \"" );
                }
                return s.substring( pos + 1, closing );
            case '{':
                return parseMap( s.substring( pos ) );
            case '[':
                if ( type instanceof ParameterizedType )
                {
                    return parseList( s.substring( pos ), ((ParameterizedType) type).getActualTypeArguments()[0] );
                }
                else
                {
                    return parseList( s.substring( pos ), Object.class );
                }

            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                String number = s.substring( pos );
                try
                {
                    return Long.valueOf( number );
                }
                catch ( NumberFormatException e )
                {
                    return Double.valueOf( number );
                }

                //deliberate fallthrough
            case 'n':
                if ( s.charAt( pos + 1 ) == 'u' && s.charAt( pos + 2 ) == 'l' && s.charAt( pos + 3 ) == 'l' )
                {
                    return null;
                }

            case 't':
                if ( s.charAt( pos + 1 ) == 'r' && s.charAt( pos + 2 ) == 'u' && s.charAt( pos + 3 ) == 'e' )
                {
                    return Boolean.TRUE;
                }
            case 'f':
                if ( s.charAt( pos + 1 ) == 'a' && s.charAt( pos + 2 ) == 'l' && s.charAt( pos + 3 ) == 's' &&
                     s.charAt( pos + 4 ) == 'e' )
                {
                    return Boolean.FALSE;
                }

            default:
                throw new IllegalArgumentException( String.format( "%s is not a valid value", s ) );
            }
        }

        throw new IllegalArgumentException( String.format( "%s is not a valid value", s ) );
    }

    @SuppressWarnings( "unchecked" )
    static <T> List<T> parseList( String s, Type type )
    {
        int pos = 0;
        int braceCounter = 0;
        List<Object> list = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        while ( pos < s.length() )
        {

            char character = s.charAt( pos );
            switch ( character )
            {
            case ' ':
                ++pos;
                break;
            case '[':
                if ( braceCounter++ > 0 )
                {
                    builder.append( s.charAt( pos ) );
                }
                ++pos;
                break;
            case ',':
                if ( braceCounter == 1 )
                {
                    Object o = parseValue( builder.toString().trim(), type );
                    assertType(o, type);
                    list.add( o );
                    builder = new StringBuilder();
                }
                else
                {
                    builder.append( s.charAt( pos ) );
                }
                ++pos;
                break;
            case ']':
                if ( --braceCounter == 0 )
                {
                    String value = builder.toString().trim();
                    if ( !value.isEmpty() )
                    {

                        Object o = parseValue( value, type );
                        assertType(o, type);

                        list.add( o );
                    }
                }
                else
                {
                    builder.append( s.charAt( pos ) );
                }
                ++pos;
                break;
            default:
                builder.append( s.charAt( pos++ ) );
                break;
            }
        }
        if ( braceCounter != 0 )
        {
            throw new IllegalArgumentException( String.format( "%s contains unbalanced '[', ']'.", s ) );
        }

        return (List<T>) list;
    }

    private static void assertType( Object obj, Type type )
    {
        if ( obj == null )
        {
            return;
        }
        //Since type erasure has already happened here we cannot verify ParameterizedType
        if ( type instanceof Class<?> )
        {
            Class<?> clazz = (Class<?>) type;
            if ( !clazz.isAssignableFrom( obj.getClass() ) )
            {
                throw new IllegalArgumentException(
                        String.format( "Expects a list of %s but got a list of %s", clazz.getSimpleName(),
                                obj.getClass().getSimpleName() ) );
            }

        }
    }
}
