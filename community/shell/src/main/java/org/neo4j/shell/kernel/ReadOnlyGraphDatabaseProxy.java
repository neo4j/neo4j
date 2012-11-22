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
package org.neo4j.shell.kernel;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipAutoIndexer;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.PlaceboTransaction;
import org.neo4j.kernel.TransactionBuilder;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;
import org.neo4j.kernel.impl.core.RelationshipTypeHolder;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.persistence.PersistenceSource;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.traversal.OldTraverserWrapper;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;

public class ReadOnlyGraphDatabaseProxy implements GraphDatabaseService, GraphDatabaseAPI, IndexManager
{
    private final GraphDatabaseAPI actual;
    private final Transaction tx = new Transaction()
    {
        public void success()
        {
        }

        public void failure()
        {
        }

        public void finish()
        {
        }

        @Override
        public Lock acquireWriteLock( PropertyContainer entity )
        {
            return PlaceboTransaction.NO_LOCK;
        }

        @Override
        public Lock acquireReadLock( PropertyContainer entity )
        {
            return PlaceboTransaction.NO_LOCK;
        }
    };
    private final TransactionBuilder txBuilder = new TransactionBuilder()
    {
        @Override
        public TransactionBuilder unforced()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Transaction begin()
        {
            return beginTx();
        }
    };
    private final AbstractTransactionManager txManager = new AbstractTransactionManager()
    {
        @Override
        public void stop() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {
        }

        @Override
        public void shutdown() throws Throwable
        {
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public javax.transaction.Transaction suspend() throws SystemException
        {
            return null;
        }

        @Override
        public void setTransactionTimeout( int seconds ) throws SystemException
        {
            throw new ReadOnlyDbException();
        }

        @Override
        public void setRollbackOnly() throws IllegalStateException, SystemException
        {
            throw new ReadOnlyDbException();
        }

        @Override
        public void rollback() throws IllegalStateException, SecurityException, SystemException
        {
            throw new ReadOnlyDbException();
        }

        @Override
        public void resume( javax.transaction.Transaction tobj ) throws IllegalStateException,
                InvalidTransactionException,
                SystemException
        {
            throw new ReadOnlyDbException();
        }

        @Override
        public javax.transaction.Transaction getTransaction() throws SystemException
        {
            throw new ReadOnlyDbException();
        }

        @Override
        public int getStatus() throws SystemException
        {
            return Status.STATUS_NO_TRANSACTION;
        }

        @Override
        public void commit() throws HeuristicMixedException, HeuristicRollbackException, IllegalStateException,
                RollbackException, SecurityException, SystemException
        {
            throw new ReadOnlyDbException();
        }

        @Override
        public void begin() throws NotSupportedException, SystemException
        {
            throw new ReadOnlyDbException();
        }

        public int getEventIdentifier()
        {
            return 0;
        }

        @Override
        public void doRecovery() throws Throwable
        {
        }

        @Override
        public TransactionState getTransactionState()
        {
            return TransactionState.NO_STATE;
        }
    };

    public ReadOnlyGraphDatabaseProxy( GraphDatabaseAPI graphDb )
    {
        this.actual = graphDb;
    }

    public GraphDatabaseService getActualGraphDb()
    {
        return actual;
    }

    public Node readOnly( Node actual )
    {
        return new ReadOnlyNodeProxy( actual );
    }

    public Relationship readOnly( Relationship actual )
    {
        return new ReadOnlyRelationshipProxy( actual );
    }

    private static <T> T readOnly()
    {
        throw new UnsupportedOperationException( "Read only Graph Database!" );
    }

    public Transaction beginTx()
    {
        // return readOnly();
        return tx;
    }

    public Node createNode()
    {
        return readOnly();
    }

    public boolean enableRemoteShell()
    {
        throw new UnsupportedOperationException( "Cannot enable Remote Shell from Remote Shell" );
    }

    public boolean enableRemoteShell( Map<String, Serializable> initialProperties )
    {
        return enableRemoteShell();
    }

    public Iterable<Node> getAllNodes()
    {
        return nodes( actual.getAllNodes() );
    }

    public Node getNodeById( long id )
    {
        return new ReadOnlyNodeProxy( actual.getNodeById( id ) );
    }

    public Node getReferenceNode()
    {
        return new ReadOnlyNodeProxy( actual.getReferenceNode() );
    }

    public Relationship getRelationshipById( long id )
    {
        return new ReadOnlyRelationshipProxy( actual.getRelationshipById( id ) );
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        return actual.getRelationshipTypes();
    }

    public KernelEventHandler registerKernelEventHandler( KernelEventHandler handler )
    {
        return readOnly();
    }

    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return readOnly();
    }

    public void shutdown()
    {
        actual.shutdown();
    }

    public KernelEventHandler unregisterKernelEventHandler( KernelEventHandler handler )
    {
        return readOnly();
    }

    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        return readOnly();
    }

    private class ReadOnlyNodeProxy implements Node
    {
        private final Node actual;

        ReadOnlyNodeProxy( Node actual )
        {
            this.actual = actual;
        }

        @Override
        public int hashCode()
        {
            return actual.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return (obj instanceof Node) && ((Node) obj).getId() == getId();
        }

        @Override
        public String toString()
        {
            return actual.toString();
        }

        public long getId()
        {
            return actual.getId();
        }

        public Relationship createRelationshipTo( Node otherNode, RelationshipType type )
        {
            return readOnly();
        }

        public void delete()
        {
            readOnly();
        }

        public Iterable<Relationship> getRelationships()
        {
            return relationships( actual.getRelationships() );
        }

        public Iterable<Relationship> getRelationships( RelationshipType... types )
        {
            return relationships( actual.getRelationships( types ) );
        }

        public Iterable<Relationship> getRelationships( Direction direction,
                                                        RelationshipType... types )
        {
            return relationships( actual.getRelationships( direction, types ) );
        }

        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return relationships( actual.getRelationships( dir ) );
        }

        public Iterable<Relationship> getRelationships( RelationshipType type, Direction dir )
        {
            return relationships( actual.getRelationships( type, dir ) );
        }

        public Relationship getSingleRelationship( RelationshipType type, Direction dir )
        {
            return new ReadOnlyRelationshipProxy( actual.getSingleRelationship( type, dir ) );
        }

        public boolean hasRelationship()
        {
            return actual.hasRelationship();
        }

        public boolean hasRelationship( RelationshipType... types )
        {
            return actual.hasRelationship( types );
        }

        public boolean hasRelationship( Direction direction, RelationshipType... types )
        {
            return actual.hasRelationship( direction, types );
        }

        public boolean hasRelationship( Direction dir )
        {
            return actual.hasRelationship( dir );
        }

        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            return actual.hasRelationship( type, dir );
        }

        public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
                                   ReturnableEvaluator returnableEvaluator, RelationshipType relationshipType,
                                   Direction direction )
        {
            return OldTraverserWrapper.traverse( this, traversalOrder, stopEvaluator,
                    returnableEvaluator, relationshipType, direction );
        }

        public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
                                   ReturnableEvaluator returnableEvaluator, RelationshipType firstRelationshipType,
                                   Direction firstDirection, RelationshipType secondRelationshipType,
                                   Direction secondDirection )
        {
            return OldTraverserWrapper.traverse( this, traversalOrder, stopEvaluator,
                    returnableEvaluator, firstRelationshipType, firstDirection,
                    secondRelationshipType, secondDirection );
        }

        public Traverser traverse( Order traversalOrder, StopEvaluator stopEvaluator,
                                   ReturnableEvaluator returnableEvaluator, Object... relationshipTypesAndDirections )
        {
            return OldTraverserWrapper.traverse( this, traversalOrder, stopEvaluator,
                    returnableEvaluator, relationshipTypesAndDirections );
        }

        public GraphDatabaseService getGraphDatabase()
        {
            return ReadOnlyGraphDatabaseProxy.this;
        }

        public Object getProperty( String key )
        {
            return actual.getProperty( key );
        }

        public Object getProperty( String key, Object defaultValue )
        {
            return actual.getProperty( key, defaultValue );
        }

        public Iterable<String> getPropertyKeys()
        {
            return actual.getPropertyKeys();
        }

        public Iterable<Object> getPropertyValues()
        {
            return actual.getPropertyValues();
        }

        public boolean hasProperty( String key )
        {
            return actual.hasProperty( key );
        }

        public Object removeProperty( String key )
        {
            return readOnly();
        }

        public void setProperty( String key, Object value )
        {
            readOnly();
        }
    }

    private class ReadOnlyRelationshipProxy implements Relationship
    {
        private final Relationship actual;

        ReadOnlyRelationshipProxy( Relationship actual )
        {
            this.actual = actual;
        }

        @Override
        public int hashCode()
        {
            return actual.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            return (obj instanceof Relationship) && ((Relationship) obj).getId() == getId();
        }

        @Override
        public String toString()
        {
            return actual.toString();
        }

        public long getId()
        {
            return actual.getId();
        }

        public void delete()
        {
            readOnly();
        }

        public Node getEndNode()
        {
            return new ReadOnlyNodeProxy( actual.getEndNode() );
        }

        public Node[] getNodes()
        {
            return new Node[]{getStartNode(), getEndNode()};
        }

        public Node getOtherNode( Node node )
        {
            return new ReadOnlyNodeProxy( actual.getOtherNode( node ) );
        }

        public Node getStartNode()
        {
            return new ReadOnlyNodeProxy( actual.getStartNode() );
        }

        public RelationshipType getType()
        {
            return actual.getType();
        }

        public boolean isType( RelationshipType type )
        {
            return actual.isType( type );
        }

        public GraphDatabaseService getGraphDatabase()
        {
            return ReadOnlyGraphDatabaseProxy.this;
        }

        public Object getProperty( String key )
        {
            return actual.getProperty( key );
        }

        public Object getProperty( String key, Object defaultValue )
        {
            return actual.getProperty( key, defaultValue );
        }

        public Iterable<String> getPropertyKeys()
        {
            return actual.getPropertyKeys();
        }

        public Iterable<Object> getPropertyValues()
        {
            return actual.getPropertyValues();
        }

        public boolean hasProperty( String key )
        {
            return actual.hasProperty( key );
        }

        public Object removeProperty( String key )
        {
            return readOnly();
        }

        public void setProperty( String key, Object value )
        {
            readOnly();
        }
    }

    public Iterable<Node> nodes( Iterable<Node> nodes )
    {
        return new IterableWrapper<Node, Node>( nodes )
        {
            @Override
            protected Node underlyingObjectToObject( Node node )
            {
                return new ReadOnlyNodeProxy( node );
            }
        };
    }

    public Iterable<Relationship> relationships( Iterable<Relationship> relationships )
    {
        return new IterableWrapper<Relationship, Relationship>( relationships )
        {
            @Override
            protected Relationship underlyingObjectToObject( Relationship relationship )
            {
                return new ReadOnlyRelationshipProxy( relationship );
            }
        };
    }

    public boolean existsForNodes( String indexName )
    {
        return actual.index().existsForNodes( indexName );
    }

    public Index<Node> forNodes( String indexName )
    {
        return new ReadOnlyNodeIndexProxy( actual.index().forNodes( indexName, null ) );
    }

    public Index<Node> forNodes( String indexName, Map<String, String> customConfiguration )
    {
        return new ReadOnlyNodeIndexProxy( actual.index().forNodes( indexName, customConfiguration ) );
    }

    public String[] nodeIndexNames()
    {
        return actual.index().nodeIndexNames();
    }

    public boolean existsForRelationships( String indexName )
    {
        return actual.index().existsForRelationships( indexName );
    }

    public RelationshipIndex forRelationships( String indexName )
    {
        return new ReadOnlyRelationshipIndexProxy( actual.index().forRelationships( indexName, null ) );
    }

    public RelationshipIndex forRelationships( String indexName,
                                               Map<String, String> customConfiguration )
    {
        return new ReadOnlyRelationshipIndexProxy( actual.index().forRelationships( indexName, customConfiguration ) );
    }

    public String[] relationshipIndexNames()
    {
        return actual.index().relationshipIndexNames();
    }

    public IndexManager index()
    {
        return this;
    }

    public Map<String, String> getConfiguration( Index<? extends PropertyContainer> index )
    {
        return actual.index().getConfiguration( index );
    }

    public String setConfiguration( Index<? extends PropertyContainer> index, String key,
                                    String value )
    {
        throw new ReadOnlyDbException();
    }

    public String removeConfiguration( Index<? extends PropertyContainer> index, String key )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public AutoIndexer<Node> getNodeAutoIndexer()
    {
        return actual.index().getNodeAutoIndexer();
    }

    @Override
    public RelationshipAutoIndexer getRelationshipAutoIndexer()
    {
        return actual.index().getRelationshipAutoIndexer();
    }

    abstract class ReadOnlyIndexProxy<T extends PropertyContainer, I extends Index<T>> implements
            Index<T>
    {
        final I actual;

        ReadOnlyIndexProxy( I actual )
        {
            this.actual = actual;
        }

        abstract T wrap( T actual );

        public void delete()
        {
            readOnly();
        }

        public void add( T entity, String key, Object value )
        {
            readOnly();
        }

        public T putIfAbsent( T entity, String key, Object value )
        {
            readOnly();
            return null;
        }

        public IndexHits<T> get( String key, Object value )
        {
            return new ReadOnlyIndexHitsProxy<T>( this, actual.get( key, value ) );
        }

        public IndexHits<T> query( String key, Object queryOrQueryObject )
        {
            return new ReadOnlyIndexHitsProxy<T>( this, actual.query( key, queryOrQueryObject ) );
        }

        public IndexHits<T> query( Object queryOrQueryObject )
        {
            return new ReadOnlyIndexHitsProxy<T>( this, actual.query( queryOrQueryObject ) );
        }

        public void remove( T entity, String key, Object value )
        {
            readOnly();
        }

        public void remove( T entity, String key )
        {
            readOnly();
        }

        public void remove( T entity )
        {
            readOnly();
        }

        @Override
        public boolean isWriteable()
        {
            return false;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return actual.getGraphDatabase();
        }
    }

    class ReadOnlyNodeIndexProxy extends ReadOnlyIndexProxy<Node, Index<Node>>
    {
        ReadOnlyNodeIndexProxy( Index<Node> actual )
        {
            super( actual );
        }

        @Override
        Node wrap( Node actual )
        {
            return readOnly( actual );
        }

        public String getName()
        {
            return actual.getName();
        }

        public Class<Node> getEntityType()
        {
            return Node.class;
        }
    }

    ;

    class ReadOnlyRelationshipIndexProxy extends
            ReadOnlyIndexProxy<Relationship, RelationshipIndex> implements RelationshipIndex
    {
        ReadOnlyRelationshipIndexProxy( RelationshipIndex actual )
        {
            super( actual );
        }

        @Override
        Relationship wrap( Relationship actual )
        {
            return readOnly( actual );
        }

        public IndexHits<Relationship> get( String key, Object valueOrNull, Node startNodeOrNull,
                                            Node endNodeOrNull )
        {
            return new ReadOnlyIndexHitsProxy<Relationship>( this, actual.get( key, valueOrNull,
                    startNodeOrNull, endNodeOrNull ) );
        }

        public IndexHits<Relationship> query( String key, Object queryOrQueryObjectOrNull,
                                              Node startNodeOrNull, Node endNodeOrNull )
        {
            return new ReadOnlyIndexHitsProxy<Relationship>( this, actual.query( key,
                    queryOrQueryObjectOrNull, startNodeOrNull, endNodeOrNull ) );
        }

        public IndexHits<Relationship> query( Object queryOrQueryObjectOrNull,
                                              Node startNodeOrNull, Node endNodeOrNull )
        {
            return new ReadOnlyIndexHitsProxy<Relationship>( this, actual.query(
                    queryOrQueryObjectOrNull, startNodeOrNull, endNodeOrNull ) );
        }

        public String getName()
        {
            return actual.getName();
        }

        public Class<Relationship> getEntityType()
        {
            return Relationship.class;
        }
    }

    private static class ReadOnlyIndexHitsProxy<T extends PropertyContainer> implements
            IndexHits<T>
    {
        private final ReadOnlyIndexProxy<T, ?> index;
        private final IndexHits<T> actual;

        ReadOnlyIndexHitsProxy( ReadOnlyIndexProxy<T, ?> index, IndexHits<T> actual )
        {
            this.index = index;
            this.actual = actual;
        }

        public void close()
        {
            actual.close();
        }

        public T getSingle()
        {
            return index.wrap( actual.getSingle() );
        }

        public int size()
        {
            return actual.size();
        }

        public boolean hasNext()
        {
            return actual.hasNext();
        }

        public T next()
        {
            return index.wrap( actual.next() );
        }

        public void remove()
        {
            readOnly();
        }

        public Iterator<T> iterator()
        {
            return this;
        }

        public float currentScore()
        {
            return actual.currentScore();
        }
    }

    @Override
    public DependencyResolver getDependencyResolver()
    {
        return actual.getDependencyResolver();
    }

    @Override
    public NodeManager getNodeManager()
    {
        return actual.getNodeManager();
    }

    @Override
    public LockManager getLockManager()
    {
        return actual.getLockManager();
    }

    @Override
    public XaDataSourceManager getXaDataSourceManager()
    {
        return actual.getXaDataSourceManager();
    }

    @Override
    public TransactionManager getTxManager()
    {
        return txManager;
    }

    @Override
    public DiagnosticsManager getDiagnosticsManager()
    {
        return actual.getDiagnosticsManager();
    }

    @Override
    public StringLogger getMessageLog()
    {
        return actual.getMessageLog();
    }

    @Override
    public RelationshipTypeHolder getRelationshipTypeHolder()
    {
        return actual.getRelationshipTypeHolder();
    }

    @Override
    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return actual.getIdGeneratorFactory();
    }

    @Override
    public String getStoreDir()
    {
        return actual.getStoreDir();
    }

    @Override
    public KernelData getKernelData()
    {
        return actual.getKernelData();
    }

    @Override
    public TransactionBuilder tx()
    {
        return actual.tx();
    }

    @Override
    public PersistenceSource getPersistenceSource()
    {
        return actual.getPersistenceSource();
    }

    @Override
    public KernelPanicEventGenerator getKernelPanicGenerator()
    {
        return actual.getKernelPanicGenerator();
    }

    @Override
    public Guard getGuard()
    {
        return actual.getGuard();
    }

    @Override
    public StoreId getStoreId()
    {
        return actual.getStoreId();
    }

    @Override
    public TxIdGenerator getTxIdGenerator()
    {
        return actual.getTxIdGenerator();
    }
}
