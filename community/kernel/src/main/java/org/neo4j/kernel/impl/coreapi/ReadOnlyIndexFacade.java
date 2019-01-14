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
package org.neo4j.kernel.impl.coreapi;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;

/**
 * Wraps an explicit index to prevent writes to it - exposing it as a read-only index.
 */
public class ReadOnlyIndexFacade<T extends PropertyContainer> implements Index<T>
{
    private final ReadableIndex<T> delegate;

    public ReadOnlyIndexFacade( ReadableIndex<T> delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public String getName()
    {
        return delegate.getName();
    }

    @Override
    public Class<T> getEntityType()
    {
        return delegate.getEntityType();
    }

    @Override
    public IndexHits<T> get( String key, Object value )
    {
        return delegate.get( key, value );
    }

    @Override
    public IndexHits<T> query( String key, Object queryOrQueryObject )
    {
        return delegate.query( key, queryOrQueryObject );
    }

    @Override
    public IndexHits<T> query( Object queryOrQueryObject )
    {
        return delegate.query( queryOrQueryObject );
    }

    private UnsupportedOperationException readOnlyIndex()
    {
        return new UnsupportedOperationException( "read only index" );
    }

    @Override
    public void add( T entity, String key, Object value )
    {
        throw readOnlyIndex();
    }

    @Override
    public T putIfAbsent( T entity, String key, Object value )
    {
        throw readOnlyIndex();
    }

    @Override
    public void remove( T entity, String key, Object value )
    {
        throw readOnlyIndex();
    }

    @Override
    public void remove( T entity, String key )
    {
        throw readOnlyIndex();
    }

    @Override
    public void remove( T entity )
    {
        throw readOnlyIndex();
    }

    @Override
    public void delete()
    {
        throw readOnlyIndex();
    }

    @Override
    public boolean isWriteable()
    {
        return false;
    }

    @Override
    public GraphDatabaseService getGraphDatabase()
    {
        return delegate.getGraphDatabase();
    }
}
