/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import java.util.Iterator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.PropertyContainer;

abstract class RemotePropertyContainer implements PropertyContainer
{
    final RemoteGraphDbEngine engine;
    final long id;

    RemotePropertyContainer( RemoteGraphDbEngine engine, long id )
    {
        this.engine = engine;
        this.id = id;
    }

    public GraphDatabaseService getGraphDatabase()
    {
        return engine.graphDb;
    }

    public final Object getProperty( String key )
    {
        Object result = getContainerProperty( key );
        if ( result == null )
        {
            throw new NotFoundException( this + " has no property \"" + key
                + "\"." );
        }
        return result;
    }

    public final Object getProperty( String key, Object defaultValue )
    {
        Object result = getContainerProperty( key );
        if ( result != null )
        {
            return result;
        }
        else
        {
            return defaultValue;
        }
    }

    abstract Object getContainerProperty( String key );

    public final Iterable<Object> getPropertyValues()
    {
        return new Iterable<Object>()
        {
            public Iterator<Object> iterator()
            {
                return new Iterator<Object>()
                {
                    final Iterator<String> keys = getPropertyKeys().iterator();

                    public boolean hasNext()
                    {
                        return keys.hasNext();
                    }

                    public Object next()
                    {
                        return getProperty( keys.next() );
                    }

                    public void remove()
                    {
                        keys.remove();
                    }
                };
            }
        };
    }
}
