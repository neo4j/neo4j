/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.schema;

import org.neo4j.kernel.api.TokenNameLookup;

import static java.lang.String.format;

public class SchemaUtil
{
    private SchemaUtil()
    {
    }

    public static String niceProperties( TokenNameLookup tokenNameLookup, int[] propertyIds )
    {
        return niceProperties( tokenNameLookup, propertyIds, "", false );
    }

    public static String niceProperties( TokenNameLookup tokenNameLookup, int[] propertyIds, String prefix,
            boolean useBrackets )
    {
        StringBuilder properties = new StringBuilder();
        if ( useBrackets )
        {
            properties.append( "(" );
        }
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            if ( i > 0 )
            {
                properties.append( ", " );
            }
            properties.append( prefix ).append( tokenNameLookup.propertyKeyGetName( propertyIds[i] ) );
        }
        if ( useBrackets )
        {
            properties.append( ")" );
        }
        return properties.toString();
    }

    public static TokenNameLookup idTokenNameLookup = new TokenNameLookup()
    {

        @Override
        public String labelGetName( int labelId )
        {
            return format( "label[%d]", labelId );
        }

        @Override
        public String relationshipTypeGetName( int relationshipTypeId )
        {
            return format( "relType[%d]", relationshipTypeId );
        }

        @Override
        public String propertyKeyGetName( int propertyKeyId )
        {
            return format( "property[%d]", propertyKeyId );
        }
    };
}
