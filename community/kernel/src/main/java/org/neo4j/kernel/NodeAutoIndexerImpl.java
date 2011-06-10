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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.AutoIndex;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

class NodeAutoIndexerImpl implements TransactionEventHandler<Void>,
        AutoIndexer<Node>
{
    static final String NODE_INDEX_NAME = "node_auto_index";

    private final Set<String> nodePropertyKeysToInclude = new HashSet<String>();
    private final Set<String> nodePropertyKeysToIgnore = new HashSet<String>();

    private final EmbeddedGraphDbImpl gdb;

    private volatile boolean enabled;

    public NodeAutoIndexerImpl( EmbeddedGraphDbImpl gdb )
    {
        this.gdb = gdb;
        resolveConfig();
    }

    public Void beforeCommit( TransactionData data ) throws Exception
    {
        if ( nodePropertyKeysToInclude.size() > 0 )
        {
            handleNodePropertiesDefaultInclude( data.removedNodeProperties(),
                    data.assignedNodeProperties() );
        }
        else
        {
            handleNodePropertiesDefaultIgnore( data.removedNodeProperties(),
                    data.assignedNodeProperties() );
        }
        return null;
    }

    private <T extends PropertyContainer> void handleNodePropertiesDefaultInclude(
            Iterable<PropertyEntry<Node>> removed, Iterable<PropertyEntry<Node>> assigned )
    {
        final Index<Node> nodeIndex = getNodeIndexInternal();
        for ( PropertyEntry<Node> entry : assigned )
        {
            if ( nodePropertyKeysToInclude.contains( entry.key() ) )
            {
                Object previousValue = entry.previouslyCommitedValue();
                String key = entry.key();
                if ( previousValue != null )
                {
                    nodeIndex.remove( entry.entity(), key, previousValue );
                }
                nodeIndex.add( entry.entity(), key, entry.value() );
            }
        }
        for ( PropertyEntry<Node> entry : removed )
        {
            // will fix thread safety later
            if ( nodePropertyKeysToInclude.contains( entry.key() ) )
            {
                Object previouslyCommitedValue = entry.previouslyCommitedValue();
                if ( previouslyCommitedValue != null )
                {
                    nodeIndex.remove( entry.entity(), entry.key(), previouslyCommitedValue );
                }
            }
        }
    }

    private <T extends PropertyContainer> void handleNodePropertiesDefaultIgnore(
            Iterable<PropertyEntry<Node>> removed,
            Iterable<PropertyEntry<Node>> assigned )
    {
        final Index<Node> nodeIndex = getNodeIndexInternal();
        for ( PropertyEntry<Node> entry : assigned )
        {
            if ( !nodePropertyKeysToIgnore.contains( entry.key() ) )
            {
                Object previousValue = entry.previouslyCommitedValue();
                String key = entry.key();
                if ( previousValue != null )
                {
                    nodeIndex.remove( entry.entity(), key, previousValue );
                }
                nodeIndex.add( entry.entity(), key, entry.value() );
            }
        }
        for ( PropertyEntry<Node> entry : removed )
        {
            // will fix thread safety later
            if ( !nodePropertyKeysToIgnore.contains( entry.key() ) )
            {
                Object previouslyCommitedValue = entry.previouslyCommitedValue();
                if ( previouslyCommitedValue != null )
                {
                    nodeIndex.remove( entry.entity(), entry.key(),
                            previouslyCommitedValue );
                }
            }
        }
    }

    private Index<Node> getNodeIndexInternal()
    {
        return gdb.index().forNodes( NODE_INDEX_NAME );
    }

    @Override
    public AutoIndex<Node> getAutoIndex()
    {
        return new IndexWrapper<Node>( getNodeIndexInternal() );
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
    public void startAutoIndexingProperty( String propName )
    {
        nodePropertyKeysToInclude.add( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void stopAutoIndexingProperty( String propName )
    {
        nodePropertyKeysToInclude.remove( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void startIgnoringProperty( String propName )
    {
        nodePropertyKeysToIgnore.add( propName );
        if ( enabled )
        {
            checkListConsistency();
        }
    }

    @Override
    public void stopIgnoringProperty( String propName )
    {
        nodePropertyKeysToIgnore.remove( propName );
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