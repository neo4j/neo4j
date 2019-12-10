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
package org.neo4j.token.api;

import java.util.function.IntFunction;

import org.neo4j.common.TokenNameLookup;

public final class TokenIdPrettyPrinter
{
    private TokenIdPrettyPrinter()
    {
    }

    public static String label( int id )
    {
        return id == TokenConstants.ANY_LABEL ? "" : (":label=" + id);
    }

    public static String propertyKey( int id )
    {
        return id == TokenConstants.ANY_PROPERTY_KEY ? "" : (":propertyKey=" + id);
    }

    public static String relationshipType( int id )
    {
        return id == TokenConstants.ANY_RELATIONSHIP_TYPE ? "" : ("[:type=" + id + "]");
    }

    public static String niceProperties( TokenNameLookup tokenNameLookup, int[] propertyIds )
    {
        return niceProperties( tokenNameLookup, propertyIds, "" );
    }

    public static String niceProperties( TokenNameLookup lookup, int[] propertyIds, String prefix )
    {
        StringBuilder out = new StringBuilder();
        out.append( '(' );
        format( out, prefix, ", ", lookup::propertyKeyGetName, propertyIds );
        out.append( ')' );
        return out.toString();
    }

    public static String niceEntityLabels( IntFunction<String> lookup, int[] labelIds )
    {
        StringBuilder out = new StringBuilder();
        out.append( ':' );
        format( out, "", ",", lookup, labelIds );
        return out.toString();
    }

    public static void format( StringBuilder out, String prefix, String separator, IntFunction<String> lookup, int[] ids )
    {
        for ( int id : ids )
        {
            String name = lookup.apply( id );
            out.append( prefix );
            if ( name.contains( ":" ) )
            {
                out.append( '`' ).append( name ).append( '`' );
            }
            else
            {
                out.append( name );
            }
            out.append( separator );
        }
        if ( ids.length > 0 )
        {
            out.setLength( out.length() - separator.length() );
        }
    }
}
