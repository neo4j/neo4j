/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.doc.server.rest.transactional;

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
