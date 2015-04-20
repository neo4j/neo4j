/*
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
package org.neo4j.cypher.internal.compiler.v2_3.birk;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.*;

public class ResultRowImpl implements Result.ResultRow
{
    private final GraphDatabaseService db;
    private Map<String, Object> results = new HashMap<>();

    public ResultRowImpl( GraphDatabaseService db )
    {
        this.db = db;
    }

    public void setNode( String k, long id )
    {
        results.put( k, db.getNodeById(id) );
    }

    public void setRelationship( String k, long id )
    {
        results.put( k, db.getRelationshipById(id) );
    }

    public void set( String k, Object value)
    {
        results.put( k, value );
    }

    @Override
    public Object get( String key )
    {
        return get( key, Object.class );
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
    public String getString( String key )
    {
        return get( key, String.class );
    }


    @Override
    public Number getNumber( String key )
    {
        return get( key, Double.class );
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

    private <T> T get( String key, Class<T> type )
    {
        Object value = results.get( key );
        if ( value == null && !results.containsKey( key ) )
        {
            throw new IllegalArgumentException( "No column \"" + key + "\" exists" );
        }
        try
        {
            return type.cast( value );
        }
        catch ( ClassCastException e )
        {
            throw new NoSuchElementException( "The current item in column \"" + key + "\" is not a " +
                    type.getSimpleName() );
        }
    }

}
