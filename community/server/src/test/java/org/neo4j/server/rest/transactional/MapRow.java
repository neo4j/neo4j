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
package org.neo4j.server.rest.transactional;

import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;

import static org.neo4j.helpers.Exceptions.withCause;

public class MapRow implements Result.ResultRow
{
    private final Map<String, Object> map;

    public MapRow( Map<String, Object> map )
    {
        this.map = map;
    }

    private <T> T get( String key, Class<T> type )
    {
        Object value = map.get( key );
        if ( value == null )
        {
            if ( !map.containsKey( key ) )
            {
                throw new NoSuchElementException( "No such entry: " + key );
            }
        }
        try
        {
            return type.cast( value );
        }
        catch ( ClassCastException e )
        {
            throw withCause( new NoSuchElementException( "Element '" + key + "' is not a " + type.getSimpleName() ), e );
        }
    }

    @Override
    public Node getNode( String key )
    {
        return get( key, Node.class );
    }

    @Override
    public Relationship getRelationship( String key )
    {
        return get( key, Relationship.class );
    }

    @Override
    public Object get( String key )
    {
        return get( key, Object.class );
    }

    @Override
    public String getString( String key )
    {
        return get( key, String.class );
    }

    @Override
    public Number getNumber( String key )
    {
        return get( key, Number.class );
    }

    @Override
    public Boolean getBoolean( String key )
    {
        return get( key, Boolean.class );
    }

    @Override
    public Path getPath( String key )
    {
        return get( key, Path.class );
    }
}
