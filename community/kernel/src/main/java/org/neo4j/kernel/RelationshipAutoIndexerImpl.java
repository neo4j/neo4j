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
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.AutoIndex;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

class RelationshipAutoIndexerImpl implements TransactionEventHandler<Void>,
        AutoIndexer<Relationship>
{
    static final String RELATIONSHIP_INDEX_NAME = "relationship_auto_index";

    private final Set<String> relPropertyKeysToInclude = new HashSet<String>();
    private final Set<String> relPropertyKeysToIgnore = new HashSet<String>();

    private final EmbeddedGraphDbImpl gdb;

    private volatile boolean enabled;

    public RelationshipAutoIndexerImpl( EmbeddedGraphDbImpl gdb )
    {
        this.gdb = gdb;
        resolveConfig();
    }

    public Void beforeCommit( TransactionData data ) throws Exception
    {
        if ( relPropertyKeysToInclude.size() > 0 )
        {
            handleRelationshipPropertiesDefaultInclude(
                    data.removedRelationshipProperties(),
                    data.assignedRelationshipProperties() );
        }
        else
        {
            handleRelationshipPropertiesDefaultIgnore(
                    data.removedRelationshipProperties(),
                    data.assignedRelationshipProperties() );
        }
        return null;
    }

    private <T extends PropertyContainer> void handleRelationshipPropertiesDefaultInclude(
            Iterable<PropertyEntry<Relationship>> removed,
            Iterable<PropertyEntry<Relationship>> assigned )
    {
        final Index<Relationship> relIndex = getRelationshipIndexInternal();
        for ( PropertyEntry<Relationship> entry : assigned )
        {
            // will fix thread safety later
            if ( relPropertyKeysToInclude.contains( entry.key() ) )
            {
                Object previousValue = entry.previouslyCommitedValue();
                String key = entry.key();
                if ( previousValue != null )
                {
                    relIndex.remove( entry.entity(), key, previousValue );
                }
                relIndex.add( entry.entity(), key, entry.value() );
            }
        }
        for ( PropertyEntry<Relationship> entry : removed )
        {
            // will fix thread safety later
            if ( relPropertyKeysToInclude.contains( entry.key() ) )
            {
                Object previouslyCommitedValue = entry.previouslyCommitedValue();
                if ( previouslyCommitedValue != null )
                {
                    relIndex.remove( entry.entity(), entry.key(),
                            previouslyCommitedValue );
                }
            }
        }
    }

    private <T extends PropertyContainer> void handleRelationshipPropertiesDefaultIgnore(
            Iterable<PropertyEntry<Relationship>> removed,
            Iterable<PropertyEntry<Relationship>> assigned )
    {
        final Index<Relationship> relIndex = getRelationshipIndexInternal();
        for ( PropertyEntry<Relationship> entry : assigned )
        {
            // will fix thread safety later
            if ( !relPropertyKeysToIgnore.contains( entry.key() ) )
            {
                Object previousValue = entry.previouslyCommitedValue();
                String key = entry.key();
                if ( previousValue != null )
                {
                    relIndex.remove( entry.entity(), key, previousValue );
                }
                relIndex.add( entry.entity(), key, entry.value() );
            }
        }
        for ( PropertyEntry<Relationship> entry : removed )
        {
            // will fix thread safety later
            if ( !relPropertyKeysToIgnore.contains( entry.key() ) )
            {
                Object previouslyCommitedValue = entry.previouslyCommitedValue();
                if ( previouslyCommitedValue != null )
                {
                    relIndex.remove( entry.entity(), entry.key(),
                            previouslyCommitedValue );
                }
            }
        }
    }

    private Index<Relationship> getRelationshipIndexInternal()
    {
        return gdb.index().forRelationships( RELATIONSHIP_INDEX_NAME );
    }

    @Override
    public AutoIndex<Relationship> getIndex()
    {
        return new IndexWrapper<Relationship>( getRelationshipIndexInternal() );
    }

    public void afterCommit( TransactionData data, Void state )
    {
    }

    public void afterRollback( TransactionData data, Void state )
    {
    }

    @Override
    public boolean isAutoIndexingEnabled()
    {
        return enabled;
    }

    @Override
    public void setAutoIndexingEnabled( boolean enable )
    {
        // Act only if actual state change requested
        if ( enable && !this.enabled )
        {
            checkListConsistency();
            gdb.registerTransactionEventHandler( this );
        }
        else if ( !enable && this.enabled )
        {
            gdb.unregisterTransactionEventHandler( this );
        }
        this.enabled = enable;
    }

    @Override
    public void startAutoIndexingNodeProperty( String propName )
    {
        nodePropertyKeysToInclude.add( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void startAutoIndexingRelationshipProperty( String propName )
    {
        relPropertyKeysToInclude.add( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void stopAutoIndexingNodeProperty( String propName )
    {
        nodePropertyKeysToInclude.remove( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void stopAutoIndexingRelationshipProperty( String propName )
    {
        relPropertyKeysToInclude.remove( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void startIgnoringNodeProperty( String propName )
    {
        nodePropertyKeysToIgnore.add( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void stopIgnoringNodeProperty( String propName )
    {
        nodePropertyKeysToIgnore.remove( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void startIgnoringRelationshipProperty( String propName )
    {
        relPropertyKeysToIgnore.add( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void stopIgnoringRelationshipProperty( String propName )
    {
        relPropertyKeysToIgnore.remove( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    private void switchSetsConditionally( Set<String> from, Set<String> to,
            String value )
    {
        if ( !from.remove( value ) || from.isEmpty() )
        {
            to.add( value );
        }
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    private void resolveConfig()
    {
        Config config = gdb.getConfig();
        boolean enable = Boolean.parseBoolean( (String) ( config.getParams().get( Config.AUTO_INDEXING_ENABLED ) ) );
        setAutoIndexingEnabled( enable );

        nodePropertyKeysToInclude.addAll( parseConfigList( (String) ( config.getParams().get( Config.NODE_KEYS_INDEXABLE ) ) ) );
        nodePropertyKeysToIgnore.addAll( parseConfigList( (String) ( config.getParams().get( Config.NODE_KEYS_NON_INDEXABLE ) ) ) );
        relPropertyKeysToInclude.addAll( parseConfigList( (String) ( config.getParams().get( Config.RELATIONSHIP_KEYS_INDEXABLE ) ) ) );
        relPropertyKeysToIgnore.addAll( parseConfigList( (String) ( config.getParams().get( Config.RELATIONSHIP_KEYS_NON_INDEXABLE ) ) ) );
        checkListConsistency();
    }

    private void checkListConsistency()
    {

        if ( !nodePropertyKeysToInclude.isEmpty()
             && !nodePropertyKeysToIgnore.isEmpty() )
        {
            throw new IllegalArgumentException(
                    "Cannot set both add and ignore lists for node auto indexing" );
        }
        if ( !relPropertyKeysToInclude.isEmpty()
             && !relPropertyKeysToIgnore.isEmpty() )
        {
            throw new IllegalArgumentException(
                    "Cannot set both add and ignore lists for relationship auto indexing" );
        }
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

    private static class IndexWrapper<K extends PropertyContainer> implements
            AutoIndex<K>
    {

        private final Index<K> delegate;

        IndexWrapper( Index<K> delegate )
        {
            this.delegate = delegate;
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
}