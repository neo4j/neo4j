/**
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
package org.neo4j.server.rest.repr;

import java.util.Map;

import static java.lang.reflect.Array.get;
import static java.lang.reflect.Array.getLength;
import static java.util.Arrays.asList;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;

public class MapRepresentation extends MappingRepresentation
{

    private final Map value;

    public MapRepresentation( Map value )
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
            }
            else if ( val instanceof Boolean )
            {
                serializer.putBoolean( key.toString(), (Boolean) val );
            }
            else if ( val instanceof String )
            {
                serializer.putString( key.toString(), (String) val );
            }
            else if (val instanceof Path )
            {
                PathRepresentation<Path> representation = new PathRepresentation<>( (Path) val );
                serializer.putMapping( key.toString(), representation );
            }
            else if ( val instanceof Iterable )
            {
                serializer.putList( key.toString(), ObjectToRepresentationConverter.getListRepresentation( (Iterable)
                        val ) );
            }
            else if ( val instanceof Map )
            {
                serializer.putMapping( key.toString(), ObjectToRepresentationConverter.getMapRepresentation( (Map)
                        val ) );
            }
            else if (val == null)
            {
                serializer.putString( key.toString(), null );
            }
            else if (val.getClass().isArray())
            {
                Object[] objects = toArray( val );

                serializer.putList( key.toString(), ObjectToRepresentationConverter.getListRepresentation( asList(objects) ) );
            }
            else if (val instanceof Node || val instanceof Relationship )
            {
                Representation representation = ObjectToRepresentationConverter.getSingleRepresentation( val );
                serializer.putMapping( key.toString(), (MappingRepresentation) representation );
            }
            else
            {
                throw new IllegalArgumentException( "Unsupported value type: " + val.getClass() );
            }
        }
    }

    private Object[] toArray( Object val )
    {
        int length = getLength( val );

        Object[] objects = new Object[length];

        for (int i=0; i<length; i++) {
            objects[i] = get( val, i );
        }

        return objects;
    }
}
