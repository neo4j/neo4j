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
import java.util.NoSuchElementException;

import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.remote.RemoteResponse.ResponseBuilder;
import org.neo4j.index.IndexHits;
import org.neo4j.index.IndexService;

/**
 * A Basic implementation of a Server for a remote graph database. This implementation relies
 * on the {@link GraphDatabaseService} API to perform the actions of the remote graph database
 * communication protocol.
 * 
 * To make a concrete implementation the subclass needs to implement the two
 * abstract methods that provide a {@link GraphDatabaseService} implementation upon
 * connection. One for authenticated connection and one for unauthenticated
 * connection. One also needs to provide the transaction manager used by the
 * {@link GraphDatabaseService} to the constructor of the server.
 * 
 * @author Tobias Ivarsson
 */
public abstract class BasicGraphDatabaseServer implements ConnectionTarget
{
    private static final int DEFAULT_BATCH_SIZE = 10;
    private final TransactionManager txManager;
    private IndexSpec[] indexes = {};

    /**
     * Create a new server for a remote graph database.
     * @param txManager
     *            The transaction manager to use on the server.
     */
    protected BasicGraphDatabaseServer( TransactionManager txManager )
    {
        if ( txManager == null )
        {
            throw new NullPointerException(
                "No transaction manager was supplied." );
        }
        this.txManager = txManager;
    }

    /**
     * Create an unauthenticated connection.
     * @return The {@link GraphDatabaseService} implementation to use for the connection.
     */
    protected abstract GraphDatabaseService connectGraphDatabase();

    /**
     * Create an authenticated connection.
     * @param username
     *            The name of the authenticating user.
     * @param password
     *            The password for the authenticating user.
     * @return The {@link GraphDatabaseService} implementation to use for the connection.
     */
    protected abstract GraphDatabaseService connectGraphDatabase( String username, String password );

    /**
     * Get the size of the next batch of {@link Node}s sent to the client in an
     * iteration.
     * 
     * Override to change the default batch size or create a smarter batching
     * scheme.
     * @param returned
     *            The number of previously returned elements in the iteration.
     * @return The size of the next batch of elements to send to the client.
     */
    protected int getNodesBatchSize( int returned )
    {
        return DEFAULT_BATCH_SIZE;
    }

    int nodesBatchSize( int returned )
    {
        int result;
        try
        {
            result = getNodesBatchSize( returned );
        }
        catch ( Exception ex )
        {
            result = DEFAULT_BATCH_SIZE;
        }
        return ( result < 1 ) ? 1 : result;
    }

    /**
     * Get the size of the next batch of {@link RelationshipType}s sent to the
     * client in an iteration.
     * 
     * Override to change the default batch size or create a smarter batching
     * scheme.
     * @param returned
     *            The number of previously returned elements in the iteration.
     * @return The size of the next batch of elements to send to the client.
     */
    protected int getTypesBatchSize( int returned )
    {
        return DEFAULT_BATCH_SIZE;
    }

    final int typesBatchSize( int returned )
    {
        int result;
        try
        {
            result = getTypesBatchSize( returned );
        }
        catch ( Exception ex )
        {
            result = DEFAULT_BATCH_SIZE;
        }
        return ( result < 1 ) ? 1 : result;
    }

    /**
     * Get the size of the next batch of {@link Relationship}s sent to the
     * client in an iteration.
     * 
     * Override to change the default batch size or create a smarter batching
     * scheme.
     * @param returned
     *            The number of previously returned elements in the iteration.
     * @return The size of the next batch of elements to send to the client.
     */
    protected int getRelationshipsBatchSize( int returned )
    {
        return DEFAULT_BATCH_SIZE;
    }

    final int relationshipsBatchSize( int returned )
    {
        int result;
        try
        {
            result = getRelationshipsBatchSize( returned );
        }
        catch ( Exception ex )
        {
            result = DEFAULT_BATCH_SIZE;
        }
        return ( result < 1 ) ? 1 : result;
    }

    /**
     * Get the size of the next batch of property keys sent to the client in an
     * iteration.
     * 
     * Override to change the default batch size or create a smarter batching
     * scheme.
     * @param returned
     *            The number of previously returned elements in the iteration.
     * @return The size of the next batch of elements to send to the client.
     */
    protected int getKeysBatchSize( int returned )
    {
        return DEFAULT_BATCH_SIZE;
    }

    final int keysBatchSize( int returned )
    {
        int result;
        try
        {
            result = getKeysBatchSize( returned );
        }
        catch ( Exception ex )
        {
            result = DEFAULT_BATCH_SIZE;
        }
        return ( result < 1 ) ? 1 : result;
    }

    /**
     * Register a server side index service implementation.
     * @param name
     *            A name that identifies the index service implementation.
     * @param index
     *            The index service implementation to register.
     */
    public void registerIndexService( String name, IndexService index )
    {
        synchronized ( this )
        {
            IndexSpec[] new_indexes = new IndexSpec[ indexes.length + 1 ];
            for ( int i = 0; i < indexes.length; i++ )
            {
                if ( indexes[ i ].name.equals( name ) )
                {
                    throw new IllegalArgumentException( "IndexService \""
                        + name + "\" is already registered." );
                }
                new_indexes[ i ] = indexes[ i ];
            }
            new_indexes[ indexes.length ] = new IndexSpec( name, index );
            this.indexes = new_indexes;
        }
    }

    private static class IndexSpec
    {
        final String name;
        final IndexService index;

        IndexSpec( String name, IndexService index )
        {
            this.name = name;
            this.index = index;
        }
    }

    void buildResponse( GraphDatabaseService neo, ResponseBuilder builder )
    {
        // TODO: implement this. Might need redefined interface.
        // This is where extra information for the caches is sent to the client.
    }

    void buildResponse( GraphDatabaseService neo, Object transactionId,
        ResponseVisitor responseState )
    {
        // TODO: implement this. Might need redefined interface.
        // This is where extra information for the caches is sent to the client.
    }

    public final RemoteConnection connect()
    {
        return new BasicConnection( this, connectGraphDatabase() );
    }

    public RemoteConnection connect( String username, String password )
    {
        return new BasicConnection( this, connectGraphDatabase( username, password ) );
    }

    private void suspendTransaction()
    {
        try
        {
            if ( txManager.getTransaction() != null )
            {
                txManager.suspend();
            }
        }
        catch ( SystemException ex )
        {
            throw new RuntimeException(
                "TODO: better exception. SystemException in tx suspend.", ex );
        }
    }

    void resumeTransaction( Transaction transaction )
    {
        suspendTransaction();
        try
        {
            txManager.resume( transaction );
        }
        catch ( InvalidTransactionException ex )
        {
            throw new RuntimeException(
                "TODO: better exception. InvalidTransactionException in tx resume.",
                ex );
        }
        catch ( IllegalStateException ex )
        {
            throw new RuntimeException(
                "TODO: better exception. IllegalStateException in tx resume.",
                ex );
        }
        catch ( SystemException ex )
        {
            throw new RuntimeException(
                "TODO: better exception. SystemException in tx resume.", ex );
        }
    }

    BasicServerTransaction beginTransaction( BasicConnection connection )
    {
        suspendTransaction();
        final Transaction tx;
        try
        {
            txManager.begin();
            tx = txManager.getTransaction();
        }
        catch ( SystemException ex )
        {
            throw new RuntimeException(
                "TODO: better exception. SystemException in tx begin.", ex );
        }
        catch ( NotSupportedException ex )
        {
            throw new RuntimeException(
                "TODO: better exceptio. txManager.begin() not supported.", ex );
        }
        return new BasicServerTransaction( this, connection, tx );
    }

    // Graph database actions

    SimpleIterator<String> getRelationshipTypes( GraphDatabaseService neo )
    {
        final Iterator<RelationshipType> types;
        if ( neo instanceof EmbeddedGraphDatabase )
        {
            types = ( ( EmbeddedGraphDatabase ) neo ).getRelationshipTypes().iterator();
        }
        else
        {
            throw new UnsupportedOperationException(
                "Cannot get the relationship types from this graph database server." );
        }
        return new SimpleIterator<String>()
        {
            @Override
            boolean hasNext()
            {
                return types.hasNext();
            }

            @Override
            String getNext()
            {
                return types.next().name();
            }
        };
    }

    long createNode( GraphDatabaseService neo )
    {
        return neo.createNode().getId();
    }

    long getReferenceNode( GraphDatabaseService neo )
    {
        return neo.getReferenceNode().getId();
    }

    SimpleIterator<NodeSpecification> getAllNodes( GraphDatabaseService neo )
    {
        final Iterator<Node> nodes = neo.getAllNodes().iterator();
        return new SimpleIterator<NodeSpecification>()
        {
            @Override
            boolean hasNext()
            {
                return nodes.hasNext();
            }

            @Override
            NodeSpecification getNext()
            {
                return new NodeSpecification( nodes.next().getId() );
            }
        };
    }

    boolean hasNodeWithId( GraphDatabaseService neo, long nodeId )
    {
        Node node = null;
        try
        {
            node = neo.getNodeById( nodeId );
        }
        catch ( Exception ex )
        {
        };
        return node != null;
    }

    void deleteNode( GraphDatabaseService neo, long nodeId )
    {
        neo.getNodeById( nodeId ).delete();
    }

    long createRelationship( GraphDatabaseService neo, String relationshipTypeName,
        long startNodeId, long endNodeId )
    {
        return neo.getNodeById( startNodeId ).createRelationshipTo(
            neo.getNodeById( endNodeId ), new RelType( relationshipTypeName ) )
            .getId();
    }

    RelationshipSpecification getRelationshipById( GraphDatabaseService neo,
        long relationshipId )
    {
        return new RelationshipSpecification( neo
            .getRelationshipById( relationshipId ) );
    }

    SimpleIterator<RelationshipSpecification> getAllRelationships(
        GraphDatabaseService neo, long nodeId, Direction direction )
    {
        final Iterator<Relationship> relationships = neo.getNodeById( nodeId )
            .getRelationships( direction ).iterator();
        return new SimpleIterator<RelationshipSpecification>()
        {
            @Override
            boolean hasNext()
            {
                return relationships.hasNext();
            }

            @Override
            RelationshipSpecification getNext()
            {
                return new RelationshipSpecification( relationships.next() );
            }
        };
    }

    SimpleIterator<RelationshipSpecification> getRelationships( GraphDatabaseService neo,
        final long nodeId, final Direction direction,
        String[] relationshipTypeNames )
    {
        RelationshipType[] types = new RelationshipType[ relationshipTypeNames.length ];
        for ( int i = 0; i < types.length; i++ )
        {
            types[ i ] = new RelType( relationshipTypeNames[ i ] );
        }
        final Iterator<Relationship> relationships = neo.getNodeById( nodeId )
            .getRelationships( types ).iterator();
        return new SimpleIterator<RelationshipSpecification>()
        {
            Relationship next = null;

            @Override
            boolean hasNext()
            {
                while ( next == null && relationships.hasNext() )
                {
                    Relationship candidate = relationships.next();
                    switch ( direction )
                    {
                        case OUTGOING:
                            if ( candidate.getStartNode().getId() == nodeId )
                            {
                                next = candidate;
                                return true;
                            }
                            break;
                        case INCOMING:
                            if ( candidate.getEndNode().getId() == nodeId )
                            {
                                next = candidate;
                                return true;
                            }
                            break;
                        case BOTH:
                            next = candidate;
                            return true;
                        default:
                            throw new IllegalArgumentException();
                    }
                }
                return next != null;
            }

            @Override
            RelationshipSpecification getNext()
            {
                try
                {
                    return new RelationshipSpecification( next );
                }
                finally
                {
                    next = null;
                }
            }
        };
    }

    void deleteRelationship( GraphDatabaseService neo, long relationshipId )
    {
        neo.getRelationshipById( relationshipId ).delete();
    }

    Object getNodeProperty( GraphDatabaseService neo, long nodeId, String key )
    {
        return neo.getNodeById( nodeId ).getProperty( key, null );
    }

    Object getRelationshipProperty( GraphDatabaseService neo, long relationshipId,
        String key )
    {
        return neo.getRelationshipById( relationshipId )
            .getProperty( key, null );
    }

    Object setNodeProperty( GraphDatabaseService neo, long nodeId, String key,
        Object value )
    {
        neo.getNodeById( nodeId ).setProperty( key, value );
        return null;
    }

    Object setRelationshipProperty( GraphDatabaseService neo, long relationshipId,
        String key, Object value )
    {
        neo.getRelationshipById( relationshipId ).setProperty( key, value );
        return null;
    }

    SimpleIterator<String> getNodePropertyKeys( GraphDatabaseService neo, long nodeId )
    {
        final Iterator<String> keys = neo.getNodeById( nodeId )
            .getPropertyKeys().iterator();
        return new SimpleIterator<String>()
        {
            @Override
            boolean hasNext()
            {
                return keys.hasNext();
            }

            @Override
            String getNext()
            {
                return keys.next();
            }
        };
    }

    SimpleIterator<String> getRelationshipPropertyKeys( GraphDatabaseService neo,
        long relationshipId )
    {
        final Iterator<String> keys = neo.getRelationshipById( relationshipId )
            .getPropertyKeys().iterator();
        return new SimpleIterator<String>()
        {
            @Override
            boolean hasNext()
            {
                return keys.hasNext();
            }

            @Override
            String getNext()
            {
                return keys.next();
            }
        };
    }

    boolean hasNodeProperty( GraphDatabaseService neo, long nodeId, String key )
    {
        return neo.getNodeById( nodeId ).hasProperty( key );
    }

    boolean hasRelationshipProperty( GraphDatabaseService neo, long relationshiId,
        String key )
    {
        return neo.getRelationshipById( relationshiId ).hasProperty( key );
    }

    Object removeNodeProperty( GraphDatabaseService neo, long nodeId, String key )
    {
        return neo.getNodeById( nodeId ).removeProperty( key );
    }

    Object removeRelationshipProperty( GraphDatabaseService neo, long relationshipId,
        String key )
    {
        return neo.getRelationshipById( relationshipId ).removeProperty( key );
    }

    int getIndexId( String indexName )
    {
        IndexSpec[] indexes = this.indexes;
        for ( int i = 0; i < indexes.length; i++ )
        {
            if ( indexes[ i ].name.equals( indexName ) )
            {
                return i;
            }
        }
        throw new NoSuchElementException( "No index with the name \""
            + indexName + "\" registered." );
    }

    SimpleIterator<NodeSpecification> getIndexNodes( GraphDatabaseService neo,
        int indexId, String key, Object value )
    {
        final IndexHits<Node> nodes = indexes[ indexId ].index.getNodes( key,
            value );
        return new SimpleIterator<NodeSpecification>( nodes.size() )
        {
            Iterator<Node> iter = nodes.iterator();

            @Override
            NodeSpecification getNext()
            {
                return new NodeSpecification( iter.next().getId() );
            }

            @Override
            boolean hasNext()
            {
                return iter.hasNext();
            }
        };
    }

    void indexNode( GraphDatabaseService neo, int indexId, long nodeId, String key,
        Object value )
    {
        indexes[ indexId ].index.index( neo.getNodeById( nodeId ), key, value );
    }

    void removeIndexNode( GraphDatabaseService neo, int indexId, long nodeId, String key,
        Object value )
    {
        indexes[ indexId ].index.removeIndex( neo.getNodeById( nodeId ), key,
            value );
    }

    public long getTotalNumberOfNodes( GraphDatabaseService neo )
    {
        if ( neo instanceof EmbeddedGraphDatabase )
        {
            EmbeddedGraphDatabase embedded = ( EmbeddedGraphDatabase ) neo;
            return embedded.getConfig().getNeoModule().getNodeManager()
                .getNumberOfIdsInUse( Node.class );
        }
        else
        {
            return -1;
        }
    }
}
