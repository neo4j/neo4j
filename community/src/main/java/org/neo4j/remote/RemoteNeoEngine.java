/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
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

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.transaction.TransactionManager;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.remote.services.TraversalService;

final class RemoteNeoEngine
{
    private class Callback implements AsynchronousCallback
    {
    }

    private static final String INDEX_SERVICE_CLASS_NAME = "org.neo4j.util.index.IndexService";

    private final ServiceDescriptor<TransactionManager> TX_DESCRIPTOR = new ServiceDescriptor<TransactionManager>()
    {
        public TransactionManager getService()
        {
            return txManager;
        }

        public String getIdentifier()
        {
            return "local transaction manager";
        }
    };
    private final ThreadLocal<RemoteTransaction> current = new ThreadLocal<RemoteTransaction>();
    private final TransactionManager txManager = null; // TODO: implement
    private final RemoteConnection connection;
    private final ConfigurationFactory factory;
    private final Map<String, RelationshipType> typesCache = null;
    private volatile TraversalService traversalService = null;
    private final Map<Class<?>, ServiceFactory<?>> serviceFactories = new HashMap<Class<?>, ServiceFactory<?>>();

    RemoteNeoEngine( RemoteConnection connection, ConfigurationModule module )
    {
        this.connection = connection;
        this.factory = new ConfigurationFactory( module, connection.configure(
            Configuration.of( module ), new Callback() ) );
        try
        {
            Class<?> indexServiceInterface = Class
                .forName( INDEX_SERVICE_CLASS_NAME );
            if ( connection instanceof IndexingConnection )
            {
                IndexingConnection ixConnection = ( IndexingConnection ) connection;
                serviceFactories.put( indexServiceInterface,
                    indexFactory( ixConnection ) );
            }
        }
        catch ( ClassNotFoundException e )
        {
        }
    }

    private ServiceFactory<?> indexFactory( IndexingConnection connection )
    {
        return new IndexServiceFactory( this, connection );
    }

    @SuppressWarnings( "unchecked" )
    private <T> ServiceDescriptor<T> txDescriptor()
    {
        return ( ServiceDescriptor<T> ) TX_DESCRIPTOR;
    }

    private TraversalService traversal()
    {
        if ( traversalService == null )
        {
            synchronized ( this )
            {
                if ( traversalService == null )
                {
                    Iterator<ServiceDescriptor<TraversalService>> candidates = getServices(
                        TraversalService.class ).iterator();
                    if ( candidates.hasNext() )
                    {
                        traversalService = candidates.next().getService();
                    }
                    else
                    {
                        traversalService = new LocalTraversalService();
                    }
                }
            }
        }
        return traversalService;
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

    <T> Iterable<ServiceDescriptor<T>> getServices( final Class<T> iface )
    {
        if ( !iface.isInterface() )
        {
            throw new IllegalArgumentException( iface + " is not an interface." );
        }
        // Transaction management for the local side is done locally
        if ( iface == TransactionManager.class )
        {
            return new Iterable<ServiceDescriptor<T>>()
            {
                public Iterator<ServiceDescriptor<T>> iterator()
                {
                    return new Iterator<ServiceDescriptor<T>>()
                    {
                        boolean hasNext = true;

                        public boolean hasNext()
                        {
                            return hasNext;
                        }

                        public ServiceDescriptor<T> next()
                        {
                            if ( !hasNext )
                            {
                                throw new NoSuchElementException();
                            }
                            hasNext = false;
                            return txDescriptor();
                        }

                        public void remove()
                        {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }
        // Discover the extension service from the server
        return new Iterable<ServiceDescriptor<T>>()
        {
            final Iterable<ServiceSpecification> services;
            {
                services = receive( connection.getServices( iface.getName() ) );
            }

            public Iterator<ServiceDescriptor<T>> iterator()
            {
                return new Iterator<ServiceDescriptor<T>>()
                {
                    Iterator<ServiceSpecification> iter = services.iterator();

                    public boolean hasNext()
                    {
                        return iter.hasNext();
                    }

                    public ServiceDescriptor<T> next()
                    {
                        return iter.next().descriptor( iface,
                            RemoteNeoEngine.this );
                    }

                    public void remove()
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    <T> ServiceFactory<T> getServiceFactory( final Class<T> iface,
        ServiceFactory.Builder builder )
    {
        @SuppressWarnings( "unchecked" )
        ServiceFactory<T> factory = ( ServiceFactory<T> ) serviceFactories
            .get( iface );
        if ( factory == null )
        {
            final HandlerFactory handlerFactory = new HandlerFactory( this,
                iface, builder );
            factory = new ServiceFactory<T>()
            {
                public T createServiceInstance( int serviceId )
                {
                    return iface.cast( Proxy.newProxyInstance( iface
                        .getClassLoader(), new Class[] { iface },
                        handlerFactory.makeServiceHandler( serviceId ) ) );
                }
            };
            serviceFactories.put( iface, factory );
        }
        return factory;
    }

    EncodedObject invokeServiceMethod( CallbackManager objectCallback,
        int serviceId, int functionIndex, EncodedObject[] arguments )
    {
        RemoteTransaction tx = current.get();
        if ( tx == null )
        {
            return receive( connection
                .invokeServiceMethod( callback( objectCallback ), serviceId,
                    functionIndex, arguments ) );
        }
        else
        {
            return tx.invokeServiceMethod( callback( objectCallback ),
                serviceId, functionIndex, arguments );
        }
    }

    EncodedObject invokeTransactionalServiceMethod( int txId,
        SynchronousCallback callback, int serviceId, int functionIndex,
        EncodedObject[] arguments )
    {
        return receive( connection.invokeTransactionalServiceMethod( txId,
            callback, serviceId, functionIndex, arguments ) );
    }

    EncodedObject invokeObjectMethod( CallbackManager objectCallback,
        int serviceId, int objectId, int functionIndex,
        EncodedObject[] arguments )
    {
        RemoteTransaction tx = current.get();
        if ( tx == null )
        {
            return receive( connection.invokeObjectMethod(
                callback( objectCallback ), serviceId, objectId, functionIndex,
                arguments ) );
        }
        else
        {
            return tx.invokeObjectMethod( callback( objectCallback ),
                serviceId, objectId, functionIndex, arguments );
        }
    }

    EncodedObject invokeTransactionalObjectMethod( int txId,
        SynchronousCallback callback, int serviceId, int objectId,
        int functionIndex, EncodedObject[] arguments )
    {
        return receive( connection.invokeTransactionalObjectMethod( txId,
            callback, serviceId, objectId, functionIndex, arguments ) );
    }

    private SynchronousCallback callback( CallbackManager objectCallback )
    {
        // TODO Auto-generated method stub
        return null;
    }

    void finalizeObject( int serviceId, int objectId )
    {
        connection.finalizeObject( serviceId, objectId );
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
        return traversal().performExternalEvaluatorTraversal( startNode, order,
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
        final IndexingConnection ixConnection, final int serviceId,
        final String key, final Object value )
    {
        return new BatchIterable<NodeSpecification>()
        {
            @Override
            IterableSpecification<NodeSpecification> init()
            {
                return receive( ixConnection.getIndexNodes( txId, serviceId,
                    key, value ) );
            }

            @Override
            IterableSpecification<NodeSpecification> more( int requestToken )
            {
                return receive( connection.getMoreNodes( txId, requestToken ) );
            }
        };
    }

    void indexNode( int txId, IndexingConnection connection, int serviceId,
        long nodeId, String key, Object value )
    {
        receive( connection.indexNode( txId, serviceId, nodeId, key, value ) );
    }

    void removeIndexNode( int txId, IndexingConnection connection,
        int serviceId, long nodeId, String key, Object value )
    {
        receive( connection.removeIndexNode( txId, serviceId, nodeId, key,
            value ) );
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
}
