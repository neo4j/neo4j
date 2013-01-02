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
package org.neo4j.server.rest.repr;

import java.util.Map;

public class GremlinMapRepresentation extends MappingRepresentation
{

    private final Map value;

    public GremlinMapRepresentation( Map value )
    {
        super( RepresentationType.MAP );
        this.value = value;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        for ( Object key : value.keySet() )
        {
            Object val = value.get( key );
            if ( val instanceof Number )
            {
                serializer.putNumber( key.toString(), (Number) val );
            } else if ( val instanceof String )
            {
                serializer.putString( key.toString(), (String) val );
            } else if ( val instanceof Iterable )
            {
                serializer.putList( key.toString(), GremlinObjectToRepresentationConverter.getListRepresentation( (Iterable) val ) );
            } else if ( val instanceof Map )
            {
                serializer.putMapping( key.toString(), GremlinObjectToRepresentationConverter.getMapRepresentation( (Map) val ) );
            }
            //default
            else
            {
                serializer.putString( key.toString(), val.toString() );
            }
        }

    }


}
