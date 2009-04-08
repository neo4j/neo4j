/*
 * Copyright 2008-2009 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser.Order;

final class RemoteNeoEngine
{
    private final ThreadLocal<RemoteTransaction> current = new ThreadLocal<RemoteTransaction>();
    private final RemoteConnection connection;
    private final ConfigurationFactory factory;
    private final Map<String, RelationshipType> typesCache = null;
    private final LocalTraversalService traversal = new LocalTraversalService();

    RemoteNeoEngine( RemoteConnection connection, ConfigurationModule module )
    {
        this.connection = connection;
        this.factory = new ConfigurationFactory( module, connection
            .configure( Configuration.of( module ) ) );
    }

    RemoteTransaction beginTx()
    {
        RemoteTransaction transaction = current.get();
        if ( transaction != null )
        {
            transaction = factory.createNestedTransaction( transaction );
        }
        else
        {
            transaction = new RemoteTransaction( this, connection
                .beginTransaction() );
        }
        current.set( transaction );
        return transaction;
    }

    void endTx( RemoteTransaction previous )
    {
        current.set( previous );
    }

    RemoteTransaction current()
    {
        return current( false );
    }

    RemoteTransaction current( boolean read_only )
    {
        RemoteTransaction transaction = current.get();
        if ( transaction == null )
        {
            throw new NotInTransactionException();
        }
        return transaction;
    }

    void shutdown()
    {
        connection.close();
    }

    private <T> T receive( RemoteResponse<T> response )
    {
        return response.value();
    }

    void commit( int txId )
    {
        connection.commit( txId );
    }

    void rollback( int txId )
    {
        connection.rollback( txId );
    }

    RelationshipType type( String name )
    {
        RelationshipType type = null;
        if ( typesCache != null )
        {
            type = typesCache.get( name );
        }
        if ( type == null )
        {
            type = new RelType( name );
            if ( typesCache != null )
            {
                typesCache.put( name, type );
            }
        }
        return type;
    }

    Iterable<String> getRelationshipTypes( final int txId )
    {
        return new BatchIterable<String>()
        {
            @Override
            IterableSpecification<String> init()
            {
                return receive( connection.getRelationshipTypes( txId ) );
            }

            @Override
            IterableSpecification<String> more( int requestToken )
            {
                return receive( connection.getMoreRelationshipTypes( txId,
                    requestToken ) );
            }
        };
    }

    // --- RemoteTransaction interface ---

    long createNode( int txId )
    {
        return receive( connection.createNode( txId ) ).id;
    }

    long getReferenceNode( int txId )
    {
        return receive( connection.getReferenceNode( txId ) ).id;
    }

    boolean hasNodeWithId( int txId, long nodeId )
    {
        return receive( connection.hasNodeWithId( txId, nodeId ) );
    }

    void deleteNode( int txId, long nodeId )
    {
        receive( connection.deleteNode( txId, nodeId ) );
    }

    RelationshipSpecification createRelationship( int txId, String typeName,
        long startId, long endId )
    {
        return receive( connection.createRelationship( txId, typeName, startId,
            endId ) );
    }

    RelationshipSpecification getRelationshipById( int txId, long relationshipId )
    {
        return receive( connection.getRelationshipById( txId, relationshipId ) );
    }

    Iterable<RelationshipSpecification> getAllRelationships( final int txId,
        final long nodeId, final Direction dir )
    {
        return new BatchIterable<RelationshipSpecification>()
        {
            @Override
            IterableSpecification<RelationshipSpecification> init()
            {
                return receive( connection.getAllRelationships( txId, nodeId,
                    dir ) );
            }

            @Override
            IterableSpecification<RelationshipSpecification> more(
                int requestToken )
            {
                return receive( connection.getMoreRelationships( txId,
                    requestToken ) );
            }
        };
    }

    Iterable<RelationshipSpecification> getRelationships( final int txId,
        final long nodeId, final Direction dir, final String[] typeNames )
    {
        return new BatchIterable<RelationshipSpecification>()
        {
            @Override
            IterableSpecification<RelationshipSpecification> init()
            {
                return receive( connection.getRelationships( txId, nodeId, dir,
                    typeNames ) );
            }

            @Override
            IterableSpecification<RelationshipSpecification> more(
                int requestToken )
            {
                return receive( connection.getMoreRelationships( txId,
                    requestToken ) );
            }
        };
    }

    void deleteRelationship( int txId, long relationshipId )
    {
        receive( connection.deleteRelationship( txId, relationshipId ) );
    }

    Iterable<TraversalPosition> traverse( final int txId, RemoteNode startNode,
        Order order, StopEvaluator stopEvaluator,
        ReturnableEvaluator returnableEvaluator,
        RelationshipType[] relationshipTypes, Direction[] directions )
    {
        return traversal.performExternalEvaluatorTraversal( startNode, order,
            stopEvaluator, returnableEvaluator, relationshipTypes, directions );
    }

    Object getNodeProperty( int txId, long nodeId, String key )
    {
        return receive( connection.getNodeProperty( txId, nodeId, key ) );
    }

    Object getRelationshipProperty( int txId, long relationshipId, String key )
    {
        return receive( connection.getRelationshipProperty( txId,
            relationshipId, key ) );
    }

    void setNodeProperty( int txId, long nodeId, String key, Object value )
    {
        receive( connection.setNodeProperty( txId, nodeId, key, value ) );
    }

    void setRelationshipProperty( int txId, long relationshipId, String key,
        Object value )
    {
        receive( connection.setRelationshipProperty( txId, relationshipId, key,
            value ) );
    }

    Iterable<String> getNodePropertyKeys( final int txId, final long nodeId )
    {
        return new BatchIterable<String>()
        {
            @Override
            IterableSpecification<String> init()
            {
                return receive( connection.getNodePropertyKeys( txId, nodeId ) );
            }

            @Override
            IterableSpecification<String> more( int requestToken )
            {
                return receive( connection.getMorePropertyKeys( txId,
                    requestToken ) );
            }
        };
    }

    Iterable<String> getRelationshipPropertyKeys( final int txId,
        final long relationshipId )
    {
        return new BatchIterable<String>()
        {
            @Override
            IterableSpecification<String> init()
            {
                return receive( connection.getRelationshipPropertyKeys( txId,
                    relationshipId ) );
            }

            @Override
            IterableSpecification<String> more( int requestToken )
            {
                return receive( connection.getMorePropertyKeys( txId,
                    requestToken ) );
            }
        };
    }

    boolean hasNodeProperty( int txId, long nodeId, String key )
    {
        return receive( connection.hasNodeProperty( txId, nodeId, key ) );
    }

    boolean hasRelationshipProperty( int txId, long relationshiId, String key )
    {
        return receive( connection.hasRelationshipProperty( txId,
            relationshiId, key ) );
    }

    Object removeNodeProperty( int txId, long nodeId, String key )
    {
        return receive( connection.removeNodeProperty( txId, nodeId, key ) );
    }

    Object removeRelationshipProperty( int txId, long relationshipId, String key )
    {
        return receive( connection.removeRelationshipProperty( txId,
            relationshipId, key ) );
    }

    // indexing

    Iterable<NodeSpecification> getIndexNodes( final int txId,
        final int indexId, final String key, final Object value )
    {
        return new BatchIterable<NodeSpecification>()
        {
            @Override
            IterableSpecification<NodeSpecification> init()
            {
                return receive( connection.getIndexNodes( txId, indexId, key,
                    value ) );
            }

            @Override
            IterableSpecification<NodeSpecification> more( int requestToken )
            {
                return receive( connection.getMoreNodes( txId, requestToken ) );
            }
        };
    }

    void indexNode( int txId, int indexId, long nodeId, String key, Object value )
    {
        receive( connection.indexNode( txId, indexId, nodeId, key, value ) );
    }

    void removeIndexNode( int txId, int indexId, long nodeId, String key,
        Object value )
    {
        receive( connection.removeIndexNode( txId, indexId, nodeId, key, value ) );
    }

    private static abstract class BatchIterable<T> implements Iterable<T>
    {
        public final Iterator<T> iterator()
        {
            final IterableSpecification<T> spec = init();
            return new Iterator<T>()
            {
                int index = 0;
                T[] content = spec.content;
                int token = spec.token;
                boolean hasMore = spec.hasMore;

                public boolean hasNext()
                {
                    return index < content.length || hasMore;
                }

                public T next()
                {
                    if ( index < content.length )
                    {
                        return content[ index++ ];
                    }
                    else if ( hasMore )
                    {
                        index = 0;
                        IterableSpecification<T> spec = more( token );
                        content = spec.content;
                        token = spec.token;
                        hasMore = spec.hasMore;
                        return content[ index++ ];
                    }
                    else
                    {
                        throw new NoSuchElementException();
                    }
                }

                public void remove()
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        abstract IterableSpecification<T> init();

        abstract IterableSpecification<T> more( int requestToken );
    }

    int getIndexId( String name )
    {
        return receive( connection.getIndexServiceId( name ) );
    }
}
