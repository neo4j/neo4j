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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.remote.RemoteResponse.ResponseBuilder;

final class BasicConnection implements RemoteConnection
{
    private final AtomicInteger txIdPool = new AtomicInteger();
    private final BasicNeoServer server;
    final NeoService neo;
    private final Map<Integer, BasicServerTransaction> transactions = new ConcurrentHashMap<Integer, BasicServerTransaction>();
    private transient boolean open = true;

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

    ResponseBuilder response()
    {
        ResponseBuilder builder = new ResponseBuilder();
        server.buildResponse( neo, builder );
        return builder;
    }

    public ClientConfigurator configure( Configuration config )
    {
        return new BasicClientConfigurator( config );
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

    public RemoteResponse<NodeSpecification> createNode( int transactionId )
    {
        return transaction( transactionId ).createNode();
    }

    public RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId )
    {
        return transaction( transactionId ).hasNodeWithId( nodeId );
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getAllNodes(
        int transactionId )
    {
        return transaction( transactionId ).getAllNodes();
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreNodes( requestToken );
    }

    public RemoteResponse<Void> closeNodeIterator( int transactionId,
        int requestToken )
    {
        return transaction( transactionId ).closeNodeIterator( requestToken );
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

    public RemoteResponse<Void> closePropertyKeyIterator( int transactionId,
        int requestToken )
    {
        return transaction( transactionId ).closePropertyKeyIterator(
            requestToken );
    }

    public RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreRelationshipTypes(
            requestToken );
    }

    public RemoteResponse<Void> closeRelationshipTypeIterator(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).closeRelationshipTypeIterator(
            requestToken );
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken )
    {
        return transaction( transactionId ).getMoreRelationships( requestToken );
    }

    public RemoteResponse<Void> closeRelationshipIterator( int transactionId,
        int requestToken )
    {
        return transaction( transactionId ).closeRelationshipIterator(
            requestToken );
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

    // Index implementation

    public RemoteResponse<Integer> getIndexServiceId( String indexName )
    {
        int id;
        try
        {
            id = server.getIndexId( indexName );
        }
        catch ( Exception ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildIntegerResponse( id );
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int indexId, String key, Object value )
    {
        return transaction( transactionId ).getIndexNodes( indexId, key, value );
    }

    public RemoteResponse<Void> indexNode( int transactionId, int indexId,
        long nodeId, String key, Object value )
    {
        return transaction( transactionId ).indexNode( indexId, nodeId, key,
            value );
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
        int indexId, long nodeId, String key, Object value )
    {
        return transaction( transactionId ).removeIndexNode( indexId, nodeId,
            key, value );
    }
}
