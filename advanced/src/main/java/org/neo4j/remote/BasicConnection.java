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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.remote.RemoteResponse.ResponseBuilder;

final class BasicConnection implements RemoteConnection, IndexingConnection
{
    private final AtomicInteger txIdPool = new AtomicInteger();
    private final BasicNeoServer server;
    final NeoService neo;
    private final Map<Integer, BasicServerTransaction> transactions = new ConcurrentHashMap<Integer, BasicServerTransaction>();
    private transient boolean open = true;
    private AsynchronousCallback callback;

    BasicConnection( BasicNeoServer server, NeoService neo )
    {
        this.server = server;
        this.neo = neo;
    }

    int allocateTransactionId()
    {
        return txIdPool.incrementAndGet();
    }

    private void checkOpen()
    {
        if ( !open )
        {
            throw new RuntimeException(
                "TODO: better exception. Connection is not open." );
        }
    }

    private BasicServerTransaction transaction( int transactionId )
    {
        checkOpen();
        BasicServerTransaction tx = transactions.get( transactionId );
        if ( tx == null )
        {
            throw new RuntimeException(
                "TODO: better exception. No such transaction." );
        }
        else
        {
            return tx;
        }
    }

    private Object service( int serviceId )
    {
        return server.getService( serviceId );
    }

    ResponseBuilder response()
    {
        ResponseBuilder builder = new ResponseBuilder();
        server.buildResponse( neo, builder );
        return builder;
    }

    public ClientConfigurator configure( Configuration config,
        AsynchronousCallback callback )
    {
        this.callback = callback;
        return new BasicClientConfigurator( config );
    }

    public RemoteResponse<Iterable<ServiceSpecification>> getServices(
        String interfaceName )
    {
        ServiceSpecification[] specifications = new ServiceSpecification[ 0 ];
        try
        {
            Iterable<ServiceSpecification> services = server
                .getServiceSpecifications( getInterface( interfaceName ) );
            if ( services != null )
            {
                List<ServiceSpecification> result = new ArrayList<ServiceSpecification>();
                for ( ServiceSpecification service : services )
                {
                    result.add( service );
                }
                specifications = result.toArray( specifications );
            }
        }
        catch ( Exception ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildServiceResponse( specifications );
    }

    private Class<?> getInterface( String interfaceName )
    {
        try
        {
            return server.getClass().getClassLoader().loadClass( interfaceName );
        }
        catch ( ClassNotFoundException e )
        {
            throw new RuntimeException(
                "TODO: Better exception type. The server does not support the service interface: "
                    + interfaceName );
        }
    }

    public RemoteResponse<EncodedObject> invokeServiceMethod(
        SynchronousCallback callback, int serviceId, int functionIndex,
        EncodedObject[] arguments )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<EncodedObject> invokeObjectMethod(
        SynchronousCallback callback, int serviceId, int objectId,
        int functionIndex, EncodedObject[] arguments )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<EncodedObject> invokeTransactionalServiceMethod(
        int transactionId, SynchronousCallback callback, int serviceId,
        int functionIndex, EncodedObject[] arguments )
    {
        return transaction( transactionId ).invokeServiceMethod( serviceId,
            functionIndex, arguments );
    }

    public RemoteResponse<EncodedObject> invokeTransactionalObjectMethod(
        int transactionId, SynchronousCallback callback, int serviceId,
        int objectId, int functionIndex, EncodedObject[] arguments )
    {
        return transaction( transactionId ).invokeObjectMethod( serviceId,
            objectId, functionIndex, arguments );
    }

    public void finalizeObject( int serviceId, int objectId )
    {
        // TODO Auto-generated method stub

    }

    public void close()
    {
        if ( open )
        {
            open = false;
            for ( BasicServerTransaction tx : transactions.values() )
            {
                transactions.remove( tx.id );
                tx.rollback();
            }
        }
    }

    public int beginTransaction()
    {
        checkOpen();
        BasicServerTransaction tx = server.beginTransaction( this );
        transactions.put( tx.id, tx );
        return tx.id;
    }

    public void commit( int transactionId )
    {
        BasicServerTransaction tx = transaction( transactionId );
        transactions.remove( tx.id );
        tx.commit();
    }

    public void rollback( int transactionId )
    {
        BasicServerTransaction tx = transaction( transactionId );
        transactions.remove( tx.id );
        tx.rollback();
    }

    public RemoteResponse<IterableSpecification<EncodedObject>> getMoreObjects(
        int requestToken )
    {
        // TODO implement this
        return null;
    }

    public RemoteResponse<IterableSpecification<EncodedObject>> getMoreObjects(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreObjects( requestToken );
    }

    public RemoteResponse<NodeSpecification> createNode( int transactionId )
    {
        return transaction( transactionId ).createNode();
    }

    public RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId )
    {
        return transaction( transactionId ).hasNodeWithId( nodeId );
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreNodes( requestToken );
    }

    public RemoteResponse<RelationshipSpecification> createRelationship(
        int transactionId, String relationshipTypeName, long startNodeId,
        long endNodeId )
    {
        return transaction( transactionId ).createRelationship(
            relationshipTypeName, startNodeId, endNodeId );
    }

    public RemoteResponse<RelationshipSpecification> getRelationshipById(
        int transactionId, long relationshipId )
    {
        return transaction( transactionId )
            .getRelationshipById( relationshipId );
    }

    public RemoteResponse<Void> deleteNode( int transactionId, long nodeId )
    {
        return transaction( transactionId ).deleteNode( nodeId );
    }

    public RemoteResponse<Void> deleteRelationship( int transactionId,
        long relationshipId )
    {
        return transaction( transactionId ).deleteRelationship( relationshipId );
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        int transactionId, long nodeId, Direction direction )
    {
        return transaction( transactionId ).getAllRelationships( nodeId,
            direction );
    }

    public RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMorePropertyKeys( requestToken );
    }

    public RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreRelationshipTypes(
            requestToken );
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreRelationships( requestToken );
    }

    public RemoteResponse<Object> getNodeProperty( int transactionId,
        long nodeId, String key )
    {
        return transaction( transactionId ).getNodeProperty( nodeId, key );
    }

    public RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        int transactionId, long nodeId )
    {
        return transaction( transactionId ).getNodePropertyKeys( nodeId );
    }

    public RemoteResponse<NodeSpecification> getReferenceNode( int transactionId )
    {
        return transaction( transactionId ).getReferenceNode();
    }

    public RemoteResponse<Object> getRelationshipProperty( int transactionId,
        long relationshipId, String key )
    {
        return transaction( transactionId ).getRelationshipProperty(
            relationshipId, key );
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        int transactionId, long relationshipId )
    {
        return transaction( transactionId ).getRelationshipPropertyKeys(
            relationshipId );
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
        int transactionId )
    {
        return transaction( transactionId ).getRelationshipTypes();
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        int transactionId, long nodeId, Direction direction,
        String[] relationshipTypeNames )
    {
        return transaction( transactionId ).getRelationships( nodeId,
            direction, relationshipTypeNames );
    }

    public RemoteResponse<Boolean> hasNodeProperty( int transactionId,
        long nodeId, String key )
    {
        return transaction( transactionId ).hasNodeProperty( nodeId, key );
    }

    public RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
        long relationshiId, String key )
    {
        return transaction( transactionId ).hasRelationshipProperty(
            relationshiId, key );
    }

    public RemoteResponse<Object> removeNodeProperty( int transactionId,
        long nodeId, String key )
    {
        return transaction( transactionId ).removeNodeProperty( nodeId, key );
    }

    public RemoteResponse<Object> removeRelationshipProperty(
        int transactionId, long relationshipId, String key )
    {
        return transaction( transactionId ).removeRelationshipProperty(
            relationshipId, key );
    }

    public RemoteResponse<Object> setNodeProperty( int transactionId,
        long nodeId, String key, Object value )
    {
        return transaction( transactionId )
            .setNodeProperty( nodeId, key, value );
    }

    public RemoteResponse<Object> setRelationshipProperty( int transactionId,
        long relationshipId, String key, Object value )
    {
        return transaction( transactionId ).setRelationshipProperty(
            relationshipId, key, value );
    }

    // IndexingConnection implementation

    public RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int serviceId, String key, Object value )
    {
        return transaction( transactionId ).getIndexNodes(
            service( serviceId ), key, value );
    }

    public RemoteResponse<Void> indexNode( int transactionId, int serviceId,
        long nodeId, String key, Object value )
    {
        return transaction( transactionId ).indexNode( service( serviceId ),
            nodeId, key, value );
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
        int serviceId, long nodeId, String key, Object value )
    {
        return transaction( transactionId ).removeIndexNode(
            service( serviceId ), nodeId, key, value );
    }
}
