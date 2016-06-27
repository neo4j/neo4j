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
package org.neo4j.kernel.impl.proc;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.neo4j.kernel.impl.proc.Neo4jValue.ntMap;

/**
 * A naive implementation of a Cypher-map/json parser. If you find yourself using this
 * for parsing huge json-document in a place where performance matters - you probably need
 * to rethink your decision.
 */
public class MapConverter implements Function<String,Neo4jValue>
{
    @Override
    public Neo4jValue apply( String s )
    {
        String value = s.trim();
        if ( value.equalsIgnoreCase( "null" ) )
        {
            return ntMap( null );
        }
        else
        {
            return ntMap( parseMap( value ) );
        }
    }

    Map<String,Object> parseMap( String s )
    {
        int pos = 0;
        int braceCounter = 0;
        Map<String,Object> map = new HashMap<>();
        StringBuilder builder = new StringBuilder();
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
                if ( braceCounter == 1 )
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

    private void addKeyValue( Map<String,Object> map, String keyValue )
    {
        if ( keyValue.isEmpty() )
        {
            return;
        }
        int split = keyValue.indexOf( ":" );
        if ( split < 0 )
        {
            throw new IllegalArgumentException( "Keys and values must be separated with ':'" );
        }
        String key = parseKey( keyValue.substring( 0, split ).trim() );
        Object value = parseValue( keyValue.substring( split + 1 ).trim() );

        if ( map.containsKey( key ) )
        {
            throw new IllegalArgumentException(
                    String.format( "Multiple occurrences of key '%s'", key ) );
        }
        map.put( key, value );
    }

    private String parseKey( String s )
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

    private Object parseValue( String s )
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
                closing = s.indexOf( '\'', pos + 1 );
                if ( closing < 0 )
                {
                    throw new IllegalArgumentException( "Did not find a matching end quote, '" );
                }
                return s.substring( pos + 1, closing );
            case '\"':
                closing = s.indexOf( '\"', pos + 1 );
                if ( closing < 0 )
                {
                    throw new IllegalArgumentException( "Did not find a matching end quote, \"" );
                }
                return s.substring( pos + 1, closing );
            case '{':
                return parseMap( s.substring( pos ) );
            case '[':
                return parseList( s.substring( pos ) );

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
                    return Long.parseLong( number );
                }
                catch ( NumberFormatException e )
                {
                    return Double.parseDouble( number );
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
                    return true;
                }
            case 'f':
                if ( s.charAt( pos + 1 ) == 'a' && s.charAt( pos + 2 ) == 'l' && s.charAt( pos + 3 ) == 's' && s.charAt( pos + 4 ) == 'e' )
                {
                    return false;
                }

            default:
                throw new IllegalArgumentException( String.format( "%s is not a valid value", s ) );
            }
        }

        throw new IllegalArgumentException( String.format( "%s is not a valid value", s ) );
    }

    Object parseList( String s )
    {
        throw new UnsupportedOperationException();
    }
}
