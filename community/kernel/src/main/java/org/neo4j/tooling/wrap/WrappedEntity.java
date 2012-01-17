/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.tooling.wrap;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;

abstract class WrappedEntity<G extends WrappedGraphDatabase, T extends PropertyContainer> implements PropertyContainer
{
    protected final G graphdb;

    WrappedEntity( G graphdb )
    {
        this.graphdb = graphdb;
    }

    protected abstract T actual();

    @Override
    public int hashCode()
    {
        return actual().hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj ) return true;
        if ( getClass().isInstance( obj ) )
        {
            WrappedEntity<?, ?> other = (WrappedEntity<?, ?>) obj;
            return actual().equals( other.actual() ) && graphdb.equals( other.graphdb );
        }
        return false;
    }

    @Override
    public String toString()
    {
        return actual().toString();
    }

    @SuppressWarnings( "unchecked" )
    static <T extends PropertyContainer> T unwrap( T entity )
    {
        if ( entity instanceof WrappedEntity<?, ?> )
        {
            return ( (WrappedEntity<?, T>) entity ).actual();
        }
        else
        {
            return entity;
        }
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return graphdb;
    }

    @Override
    public boolean hasProperty( String key )
    {
        return actual().hasProperty( key );
    }

    @Override
    public Object getProperty( String key )
    {
        return actual().getProperty( key );
    }

    @Override
    public Object getProperty( String key, Object defaultValue )
    {
        return actual().getProperty( key, defaultValue );
    }

    @Override
    public void setProperty( String key, Object value )
    {
        actual().setProperty( key, value );
    }

    @Override
    public Object removeProperty( String key )
    {
        return actual().removeProperty( key );
    }

    @Override
    public Iterable<String> getPropertyKeys()
    {
        return actual().getPropertyKeys();
    }

    @SuppressWarnings( "deprecation" )
    @Override
    public Iterable<Object> getPropertyValues()
    {
        return actual().getPropertyValues();
    }
}
