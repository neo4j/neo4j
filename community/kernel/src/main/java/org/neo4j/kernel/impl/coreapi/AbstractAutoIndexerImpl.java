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
package org.neo4j.kernel.impl.coreapi;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.kernel.PropertyTracker;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Default implementation of the AutoIndexer, binding to the beforeCommit hook
 * as a TransactionEventHandler
 *
 * @param <T> The database primitive type auto indexed/**
 */
abstract class AbstractAutoIndexerImpl<T extends PropertyContainer> implements
        PropertyTracker<T>, AutoIndexer<T>, Lifecycle
{
    protected final Set<String> propertyKeysToInclude = new HashSet<String>();

    private volatile boolean enabled;

    public AbstractAutoIndexerImpl( )
    {
    }

    public void propertyAdded( T primitive, String propertyName,
            Object propertyValue )
    {
        if ( propertyKeysToInclude.contains( propertyName ) )
        {
            getIndexInternal().add( primitive, propertyName, propertyValue );
        }
    }

    public void propertyChanged( T primitive, String propertyName,
            Object oldValue, Object newValue )
    {
        if ( propertyKeysToInclude.contains( propertyName ) )
        {
            if ( oldValue != null )
            {
                getIndexInternal().remove( primitive, propertyName, oldValue );
            }
            getIndexInternal().add( primitive, propertyName, newValue );
        }
    }

    public void propertyRemoved( T primitive, String propertyName,
            Object propertyValue )
    {
        if ( propertyKeysToInclude.contains( propertyName ) )
        {
            getIndexInternal().remove( primitive, propertyName );
        }
    }

    @Override
    public ReadableIndex<T> getAutoIndex()
    {
        return new IndexWrapper<T>( getIndexInternal() );
    }

    public void setEnabled( boolean enabled )
    {
        this.enabled = enabled;
    }

    @Override
    public boolean isEnabled()
    {
        return enabled;
    }

    @Override
    public void startAutoIndexingProperty( String propName )
    {
        propertyKeysToInclude.add(propName);
    }

    @Override
    public void stopAutoIndexingProperty( String propName )
    {
        propertyKeysToInclude.remove( propName );
    }

    @Override
    public Set<String> getAutoIndexedProperties()
    {
        return Collections.unmodifiableSet( propertyKeysToInclude );
    }

    /**
     * Returns the actual index used by the auto indexer. This is not supposed
     * to
     * leak unprotected to the outside world.
     *
     * @return The Index used by this AutoIndexer
     */
    protected abstract Index<T> getIndexInternal();

    protected Set<String> parseConfigList(String list)
    {
        if ( list == null )
        {
            return Collections.emptySet();
        }

        Set<String> toReturn = new HashSet<String>();
        StringTokenizer tokenizer = new StringTokenizer(list, "," );
        String currentToken;
        while ( tokenizer.hasMoreTokens() )
        {
            currentToken = tokenizer.nextToken();
            if ( ( currentToken = currentToken.trim() ).length() > 0 )
            {
                toReturn.add( currentToken );
            }
        }
        return toReturn;
    }

    /**
     * Simple implementation of the AutoIndex interface, as a wrapper around a
     * normal Index that exposes the read-only operations.
     *
     * @param <K> The type of database primitive this index holds
     */
    private static class IndexWrapper<K extends PropertyContainer> implements
            ReadableIndex<K>
    {
        private final Index<K> delegate;

        IndexWrapper( Index<K> delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public String getName()
        {
            return delegate.getName();
        }

        @Override
        public Class<K> getEntityType()
        {
            return delegate.getEntityType();
        }

        @Override
        public IndexHits<K> get( String key, Object value )
        {
            return delegate.get( key, value );
        }

        @Override
        public IndexHits<K> query( String key, Object queryOrQueryObject )
        {
            return delegate.query( key, queryOrQueryObject );
        }

        @Override
        public IndexHits<K> query( Object queryOrQueryObject )
        {
            return delegate.query( queryOrQueryObject );
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

    /**
     * A simple wrapper that makes a read only index into a read-write
     * index with the unsupported operations throwing
     * UnsupportedOperationException
     * Useful primarily for returning the actually read-write but
     * publicly read-only auto indexes from the get by name methods of the index
     * manager.
     */
    static class ReadOnlyIndexToIndexAdapter<T extends PropertyContainer>
            implements Index<T>
    {
        private final ReadableIndex<T> delegate;

        public ReadOnlyIndexToIndexAdapter( ReadableIndex<T> delegate )
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
}