/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.ReadableIndex;

/**
 * Default implementation of the AutoIndexer, binding to the beforeCommit hook
 * as a TransactionEventHandler
 *
 * @param <T> The database primitive type auto indexed
 */
abstract class AbstractAutoIndexerImpl<T extends PropertyContainer> implements
        TransactionEventHandler<Void>, AutoIndexer<T>
{
    private final Set<String> propertyKeysToInclude = new HashSet<String>();

    private final EmbeddedGraphDbImpl gdb;

    private volatile boolean enabled;

    public AbstractAutoIndexerImpl( EmbeddedGraphDbImpl gdb )
    {
        this.gdb = gdb;
        resolveConfig();
    }

    @Override
    public ReadableIndex<T> getAutoIndex()
    {
        return new IndexWrapper<T>( getIndexInternal() );
    }

    @Override
    public boolean isEnabled()
    {
        return enabled;
    }

    @Override
    public void setEnabled( boolean enable )
    {
        // Act only if actual state change requested
        if ( enable && !this.enabled )
        {
            gdb.registerTransactionEventHandler( this );
        }
        else if ( !enable && this.enabled )
        {
            gdb.unregisterTransactionEventHandler( this );
        }
        this.enabled = enable;
    }

    @Override
    public void startAutoIndexingProperty( String propName )
    {
        propertyKeysToInclude.add( propName );
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

    protected EmbeddedGraphDbImpl getGraphDbImpl()
    {
        return gdb;
    }

    /**
     * Returns an Iterable of the removed property entries for the primitive
     * types
     * that are of interest for this auto indexer.
     *
     * @param data The TransactionData available beforeCommmit.
     * @return An Iterable of the removed properties for the primitive supported
     *         by this AutoIndexer
     */
    protected abstract Iterable<PropertyEntry<T>> getRemovedPropertiesOnCommit(
            TransactionData data );

    /**
     * Returns an Iterable of the changed property entries for the primitive
     * types
     * that are of interest for this auto indexer.
     *
     * @param data The TransactionData available beforeCommmit.
     * @return An Iterable of the changed properties for the primitive supported
     *         by this AutoIndexer
     */
    protected abstract Iterable<PropertyEntry<T>> getAssignedPropertiesOnCommit(
            TransactionData data );

    /**
     * Returns the actual index used by the auto indexer. This is not supposed
     * to
     * leak unprotected to the outside world.
     *
     * @return The Index used by this AutoIndexer
     */
    protected abstract Index<T> getIndexInternal();

    /**
     * @return The configuration parameter name that contains the comma
     *         separated list of properties to auto index.
     */
    protected abstract String getAutoIndexConfigListName();

    /**
     * @return The configuration parameter name that sets this auto indexer to
     *         enabled/disabled.
     */
    protected abstract String getEnableConfigName();

    /**
     * @return The String that is the name of the Index used by this auto
     *         indexer for Indexing.
     */
    protected abstract String getAutoIndexName();

    /**
     * Executed when there are indexable properties defined. It indexes only
     * those defined, removing anything else.
     *
     * @param removed An iterable of PropertyEntries removed during this
     *            transaction for the Primitive type supported
     * @param assigned An iterable of PropertyEntries changed during this
     *            transaction for the Primitive type supported
     */
    private void handleProperties(
            Iterable<PropertyEntry<T>> removed,
            Iterable<PropertyEntry<T>> assigned )
    {
        final Index<T> nodeIndex = getIndexInternal();
        for ( PropertyEntry<T> entry : assigned )
        {
            Object previousValue = entry.previouslyCommitedValue();
            String key = entry.key();
            if ( propertyKeysToInclude.contains( key ) )
            {
                if ( previousValue != null )
                {
                    nodeIndex.remove( entry.entity(), key, previousValue );
                }
                nodeIndex.add( entry.entity(), key, entry.value() );
            }
            else if ( previousValue != null )
            {
                nodeIndex.remove( entry.entity(), key, previousValue );
            }
        }
        for ( PropertyEntry<T> entry : removed )
        {
            String key = entry.key();
            // will fix thread safety later
            Object previouslyCommitedValue = entry.previouslyCommitedValue();
            if ( previouslyCommitedValue != null )
            {
                nodeIndex.remove( entry.entity(), key, previouslyCommitedValue );
            }
        }
    }

    /**
     * Reads in the configuration from the GraphDbImpl, gets the actual
     * configuration parameter names from the derived implementations and parses
     * the input.
     */
    private void resolveConfig()
    {
        Config config = gdb.getConfig();
        boolean enable = Boolean.parseBoolean( (String) ( config.getParams().get( getEnableConfigName() ) ) );
        setEnabled( enable );

        propertyKeysToInclude.addAll( parseConfigList( (String) ( config.getParams().get( getAutoIndexConfigListName() ) ) ) );
    }

    private Set<String> parseConfigList( String list )
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
     * The main work method, gets the changed properties in the transactiosn
     * for the supported primitives and adds/removes them from the index.
     */
    public Void beforeCommit( TransactionData data ) throws Exception
    {
        /*
         *  We always have to run this because if there are
         *  auto indexed properties that now are not monitored
         *  any more (hence the include set is - possibly - empty)
         *  then we have to remove them from the auto index.
         */
        handleProperties( getRemovedPropertiesOnCommit( data ),
                getAssignedPropertiesOnCommit( data ) );
        return null;
    }

    @Override
    public void afterCommit( TransactionData data, Void state )
    {
    }

    @Override
    public void afterRollback( TransactionData data, Void state )
    {
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

        @Override
        public void add( T entity, String key, Object value )
        {
            throw new UnsupportedOperationException( "read only index" );
        }

        @Override
        public void remove( T entity, String key, Object value )
        {
            throw new UnsupportedOperationException( "read only index" );
        }

        @Override
        public void remove( T entity, String key )
        {
            throw new UnsupportedOperationException( "read only index" );
        }

        @Override
        public void remove( T entity )
        {
            throw new UnsupportedOperationException( "read only index" );
        }

        @Override
        public void delete()
        {
            throw new UnsupportedOperationException( "read only index" );
        }
    }
}