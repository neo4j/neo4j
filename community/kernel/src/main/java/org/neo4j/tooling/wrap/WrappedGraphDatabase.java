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
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.TransactionBuilder;
import org.neo4j.kernel.impl.util.StringLogger;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

public abstract class WrappedGraphDatabase extends AbstractGraphDatabase
{
    protected final GraphDatabaseService graphdb;

    public WrappedGraphDatabase( GraphDatabaseService graphdb )
    {
        super( storeDirOf( graphdb ) );
        this.graphdb = graphdb;
    }
    
    private static String storeDirOf( GraphDatabaseService graphdb )
    {
        graphdb.getClass(); // null check
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return ( (AbstractGraphDatabase) graphdb ).getStoreDir();
        }
        return "."; // (in-?) sane default
    }
    
    @Override
    protected StringLogger createStringLogger()
    {
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return ( (AbstractGraphDatabase) graphdb ).getMessageLog();
        }
        return super.createStringLogger();
    }

    @Override
    public final int hashCode()
    {
        return graphdb.hashCode();
    }

    @Override
    public final boolean equals( Object obj )
    {
        return this == obj || getClass().isInstance( obj ) && graphdb.equals( ( (WrappedGraphDatabase) obj ).graphdb );
    }

    @Override
    public final String toString()
    {
        return graphdb.toString();
    }

    protected abstract WrappedNode<? extends WrappedGraphDatabase> node( Node node, boolean created );

    protected abstract WrappedRelationship<? extends WrappedGraphDatabase> relationship( Relationship relationship,
            boolean created );
    
    protected Lock lock( Lock lock )
    {
        return lock;
    }

    Iterable<Node> nodes( final Iterable<Node> nodes )
    {
        return new IterableWrapper<Node, Node>( nodes )
        {
            @Override
            protected Node underlyingObjectToObject( Node object )
            {
                return node( object, false );
            }
        };
    }

    Iterable<Relationship> relationships( final Iterable<Relationship> relationships )
    {
        return new IterableWrapper<Relationship, Relationship>( relationships )
        {
            @Override
            protected Relationship underlyingObjectToObject( Relationship object )
            {
                return relationship( object, false );
            }
        };
    }

    protected WrappedIndex.WrappedNodeIndex nodeIndex( final Index<Node> index, String indexName )
    {
        return new WrappedIndex.WrappedNodeIndex( this )
        {
            @Override
            protected ReadableIndex<Node> actual()
            {
                return index;
            }
        };
    }

    protected WrappedIndex.WrappedRelationshipIndex relationshipIndex(
            final RelationshipIndex index, String indexName )
    {
        return new WrappedIndex.WrappedRelationshipIndex( this )
        {
            @Override
            protected ReadableRelationshipIndex actual()
            {
                return index;
            }
        };
    }

    protected WrappedIndex.WrappedNodeIndex automaticNodeIndex( final ReadableIndex<Node> index )
    {
        return new WrappedIndex.WrappedNodeIndex( this )
        {
            @Override
            protected ReadableIndex<Node> actual()
            {
                return index;
            }
        };
    }

    protected WrappedIndex.WrappedRelationshipIndex automaticRelationshipIndex(
            final ReadableRelationshipIndex index )
    {
        return new WrappedIndex.WrappedRelationshipIndex( this )
        {
            @Override
            protected ReadableRelationshipIndex actual()
            {
                return index;
            }
        };
    }

    protected void onBeginTransaction() throws TransactionNotAllowedException
    {
        // default: do nothing
    }

    protected void onSuccessTransaction()
    {
        // default: do nothing
    }

    protected void onFailureTransaction()
    {
        // default: do nothing
    }

    protected void onFinishTransaction()
    {
        // default: do nothing
    }

    protected void onNodeCreate()
    {
        // default: do nothing
    }

    protected void onShutdown()
    {
        // default: do nothing
    }
    
    @Override
    public final Transaction beginTx()
    {
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return (WrappedTransaction) super.beginTx();
        }
        boolean openTx;
        try
        {
            onBeginTransaction();
            openTx = true;
        }
        catch ( TransactionNotAllowedException exception )
        {
            exception.throwCause();
            openTx = false;
        }
        return new WrappedTransaction( this, openTx ? graphdb.beginTx() : null );
    }

    @Override
    public final Node createNode()
    {
        onNodeCreate();
        return node( graphdb.createNode(), true );
    }

    @Override
    public final Node getNodeById( long id )
    {
        return node( graphdb.getNodeById( id ), false );
    }

    @Override
    public final Relationship getRelationshipById( long id )
    {
        return relationship( graphdb.getRelationshipById( id ), false );
    }

    @Override
    public final Node getReferenceNode()
    {
        return node( graphdb.getReferenceNode(), false );
    }

    @Override
    public final Iterable<Node> getAllNodes()
    {
        return nodes( graphdb.getAllNodes() );
    }

    @Override
    public final Iterable<RelationshipType> getRelationshipTypes()
    {
        return graphdb.getRelationshipTypes();
    }

    @Override
    protected final void close()
    {
        try
        {
            onShutdown();
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    @SuppressWarnings( { "unchecked", "rawtypes" } )
    private final Map<TransactionEventHandler<?>, TransactionEventHandler<?>> handlers = new IdentityHashMap();

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        TransactionEventHandler<?> wrapped;
        synchronized ( handlers )
        {
            wrapped = handlers.get( handler );
            if ( wrapped == null ) handlers.put( handler, wrapped = new WrappedEventHandler<T>( this, handler ) );
        }
        graphdb.registerTransactionEventHandler( wrapped );
        return handler;
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler( TransactionEventHandler<T> handler )
    {
        TransactionEventHandler<?> wrapped;
        synchronized ( handlers )
        {
            wrapped = handlers.get( handler );
        }
        if ( wrapped == null ) wrapped = handler;
        graphdb.unregisterTransactionEventHandler( wrapped );
        return handler;
    }

    @Override
    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return graphdb.registerKernelEventHandler( handler );
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return graphdb.unregisterKernelEventHandler( handler );
    }

    private final IndexManager indexManager = new IndexManager()
    {
        @Override
        public String setConfiguration( Index<? extends PropertyContainer> index, String key, String value )
        {
            return graphdb.index().setConfiguration( WrappedIndex.unwrapIndex( index ), key, value );
        }

        @Override
        public String removeConfiguration( Index<? extends PropertyContainer> index, String key )
        {
            return graphdb.index().removeConfiguration( WrappedIndex.unwrapIndex( index ), key );
        }

        @Override
        public String[] relationshipIndexNames()
        {
            return graphdb.index().relationshipIndexNames();
        }

        @Override
        public String[] nodeIndexNames()
        {
            return graphdb.index().nodeIndexNames();
        }

        @Override
        public Map<String, String> getConfiguration( Index<? extends PropertyContainer> index )
        {
            return graphdb.index().getConfiguration( WrappedIndex.unwrapIndex( index ) );
        }

        @Override
        public RelationshipIndex forRelationships( String indexName, Map<String, String> customConfiguration )
        {
            return relationshipIndex( graphdb.index().forRelationships( indexName, customConfiguration ), indexName );
        }

        @SuppressWarnings( { "unchecked", "rawtypes" } )
        @Override
        public RelationshipIndex forRelationships( String indexName )
        {
            return relationshipIndex( graphdb.index().forRelationships( indexName ), indexName );
        }

        @SuppressWarnings( { "unchecked", "rawtypes" } )
        @Override
        public Index<Node> forNodes( String indexName, Map<String, String> customConfiguration )
        {
            return nodeIndex( graphdb.index().forNodes( indexName, customConfiguration ), indexName );
        }

        @SuppressWarnings( { "unchecked", "rawtypes" } )
        @Override
        public Index<Node> forNodes( String indexName )
        {
            return nodeIndex( graphdb.index().forNodes( indexName ), indexName );
        }

        @Override
        public boolean existsForRelationships( String indexName )
        {
            return graphdb.index().existsForRelationships( indexName );
        }

        @Override
        public boolean existsForNodes( String indexName )
        {
            return graphdb.index().existsForNodes( indexName );
        }

        @Override
        public AutoIndexer<Node> getNodeAutoIndexer()
        {
            return autoNodeIndex;
        }

        @Override
        public RelationshipAutoIndexer getRelationshipAutoIndexer()
        {
            return autoRelationshipIndex;
        }
    };

    private final AutoIndexer<Node> autoNodeIndex = new WrappedAutoIndexer<Node>()
    {
        @Override
        AutoIndexer<Node> actual()
        {
            return graphdb.index().getNodeAutoIndexer();
        }

        @Override
        public ReadableIndex<Node> getAutoIndex()
        {
            return automaticNodeIndex( actual().getAutoIndex() );
        }
    };
    private final RelationshipAutoIndexer autoRelationshipIndex = new WrappedRelationshipAutoIndex();

    private abstract class WrappedAutoIndexer<T extends PropertyContainer> implements AutoIndexer<T>
    {
        abstract AutoIndexer<T> actual();

        @Override
        public void setEnabled( boolean enabled )
        {
            actual().setEnabled( enabled );
        }

        @Override
        public boolean isEnabled()
        {
            return actual().isEnabled();
        }

        @Override
        public void startAutoIndexingProperty( String propName )
        {
            actual().startAutoIndexingProperty( propName );
        }

        @Override
        public void stopAutoIndexingProperty( String propName )
        {
            actual().stopAutoIndexingProperty( propName );
        }

        @Override
        public Set<String> getAutoIndexedProperties()
        {
            return actual().getAutoIndexedProperties();
        }
    }

    private class WrappedRelationshipAutoIndex extends WrappedAutoIndexer<Relationship> implements
            RelationshipAutoIndexer
    {
        @Override
        RelationshipAutoIndexer actual()
        {
            return graphdb.index().getRelationshipAutoIndexer();
        }

        @Override
        public ReadableRelationshipIndex getAutoIndex()
        {
            return automaticRelationshipIndex( actual().getAutoIndex() );
        }
    }

    @Override
    public IndexManager index()
    {
        return indexManager;
    }
    
    private static class WrappedTransactionBuilder extends WrappedObject<TransactionBuilder> implements
            TransactionBuilder
    {
        WrappedTransactionBuilder( WrappedGraphDatabase graphdb, TransactionBuilder wrapped )
        {
            super( graphdb, wrapped );
        }

        @Override
        public WrappedTransaction begin()
        {
            boolean openTx;
            try
            {
                graphdb.onBeginTransaction();
                openTx = true;
            }
            catch ( TransactionNotAllowedException exception )
            {
                exception.throwCause();
                openTx = false;
            }
            return new WrappedTransaction( graphdb, openTx ? wrapped.begin() : null );
        }

        @Override
        public WrappedTransactionBuilder unforced()
        {
            return new WrappedTransactionBuilder( graphdb, wrapped.unforced() );
        }
    }

    private static class WrappedTransaction extends WrappedObject<Transaction> implements Transaction
    {
        WrappedTransaction( WrappedGraphDatabase graphdb, Transaction tx )
        {
            super( graphdb, tx );
        }

        @Override
        public Lock acquireReadLock( PropertyContainer entity )
        {
            return graphdb.lock( wrapped.acquireReadLock( entity ) );
        }

        @Override
        public Lock acquireWriteLock( PropertyContainer entity )
        {
            return graphdb.lock( wrapped.acquireWriteLock( entity ) );
        }

        @Override
        public void success()
        {
            try
            {
                graphdb.onSuccessTransaction();
            }
            finally
            {
                if ( wrapped != null ) wrapped.success();
            }
        }

        @Override
        public void failure()
        {
            try
            {
                graphdb.onFailureTransaction();
            }
            finally
            {
                if ( wrapped != null ) wrapped.failure();
            }
        }

        @Override
        public void finish()
        {
            try
            {
                graphdb.onFinishTransaction();
            }
            finally
            {
                if ( wrapped != null ) wrapped.finish();
            }
        }
    }

    // AbstractGraphDatabase

    private static final String NOT_AGD = "Underlying graph database is not an AbstractGraphDatabase";

    @Override
    public TransactionBuilder tx()
    {
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return new WrappedTransactionBuilder( this, ( (AbstractGraphDatabase) graphdb ).tx() );
        }
        throw new UnsupportedOperationException( NOT_AGD );
    }
    
    @Override
    public Config getConfig()
    {
        // WARNING: this is an escape hatch to the underlying implementation
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return ( (AbstractGraphDatabase) graphdb ).getConfig();
        }
        throw new UnsupportedOperationException( NOT_AGD );
    }

    @Override
    public <T> Collection<T> getManagementBeans( Class<T> type )
    {
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return ( (AbstractGraphDatabase) graphdb ).getManagementBeans( type );
        }
        throw new UnsupportedOperationException( NOT_AGD );
    }
    
    @Override
    public KernelData getKernelData()
    {
        if ( graphdb instanceof AbstractGraphDatabase )
        {
            return ( (AbstractGraphDatabase) graphdb ).getKernelData();
        }
        throw new UnsupportedOperationException( NOT_AGD );
    }
}
