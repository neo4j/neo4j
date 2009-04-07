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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.neo4j.api.core.Direction;
import org.neo4j.remote.RemoteResponse.ResponseBuilder;

final class BasicServerTransaction
{
    private final BasicNeoServer server;
    private final Transaction transaction;
    private final BasicConnection connection;
    final Integer id;
    private int nodTokenPool = 0;
    private int relTokenPool = 0;
    private int strTokenPool = 0;
    private final Map<Integer, SimpleIterator<EncodedObject>> objIter = new HashMap<Integer, SimpleIterator<EncodedObject>>();
    private final Map<Integer, SimpleIterator<RelationshipSpecification>> relIter = new HashMap<Integer, SimpleIterator<RelationshipSpecification>>();
    private final Map<Integer, SimpleIterator<NodeSpecification>> nodIter = new HashMap<Integer, SimpleIterator<NodeSpecification>>();
    private final Map<Integer, SimpleIterator<String>> strIter = new HashMap<Integer, SimpleIterator<String>>();

    BasicServerTransaction( BasicNeoServer server, BasicConnection connection,
        Transaction transaction )
    {
        this.server = server;
        this.connection = connection;
        this.transaction = transaction;
        id = connection.allocateTransactionId();
    }

    // Transaction management

    private void resume()
    {
        server.resumeTransaction( transaction );
    }

    void commit()
    {
        resume();
        try
        {
            transaction.commit();
        }
        catch ( IllegalStateException ex )
        {
        }
        catch ( SecurityException ex )
        {
        }
        catch ( RollbackException ex )
        {
        }
        catch ( HeuristicMixedException ex )
        {
        }
        catch ( HeuristicRollbackException ex )
        {
        }
        catch ( SystemException ex )
        {
        }
    }

    void rollback()
    {
        resume();
        try
        {
            transaction.rollback();
        }
        catch ( IllegalStateException ex )
        {
        }
        catch ( SystemException e )
        {
        }
    }

    // Internal

    private ResponseBuilder response()
    {
        ResponseBuilder builder = connection.response();
        server.buildResponse( connection.neo, id, builder );
        return builder;
    }

    private SimpleIterator<EncodedObject> getObjects( int token )
    {
        return objIter.remove( token );
    }

    private RemoteResponse<IterableSpecification<EncodedObject>> objects(
        int token, SimpleIterator<EncodedObject> iterator )
    {
        EncodedObject[] result = consume( token, objIter, iterator,
            server.objectsBatchSize( iterator.count() ) ).toArray(
            new EncodedObject[ 0 ] );
        if ( iterator.hasNext() )
        {
            return response().buildPartialObjectResponse( token, result );
        }
        else
        {
            return response().buildFinalObjectResponse( result );
        }
    }

    private SimpleIterator<NodeSpecification> getNodes( int token )
    {
        return nodIter.remove( token );
    }

    private RemoteResponse<IterableSpecification<NodeSpecification>> nodes(
        int token, SimpleIterator<NodeSpecification> iterator )
    {
        NodeSpecification[] result = consume( token, nodIter, iterator,
            server.nodesBatchSize( iterator.count() ) ).toArray(
            new NodeSpecification[ 0 ] );
        if ( iterator.hasNext() )
        {
            return response().buildPartialNodeResponse( token, result );
        }
        else
        {
            return response().buildFinalNodeResponse( result );
        }
    }

    private SimpleIterator<RelationshipSpecification> getRelationships(
        int token )
    {
        return relIter.remove( token );
    }

    private RemoteResponse<IterableSpecification<RelationshipSpecification>> relationships(
        int token, SimpleIterator<RelationshipSpecification> iterator )
    {
        RelationshipSpecification[] result = consume( token, relIter, iterator,
            server.relationshipsBatchSize( iterator.count() ) ).toArray(
            new RelationshipSpecification[ 0 ] );
        if ( iterator.hasNext() )
        {
            return response().buildPartialRelationshipResponse( token, result );
        }
        else
        {
            return response().buildFinalRelationshipResponse( result );
        }
    }

    private SimpleIterator<String> getStrings( int token )
    {
        return strIter.remove( token );
    }

    private RemoteResponse<IterableSpecification<String>> strings( int token,
        SimpleIterator<String> iterator, int batchSize )
    {
        String[] result = consume( token, strIter, iterator, batchSize )
            .toArray( new String[ 0 ] );
        if ( iterator.hasNext() )
        {
            return response().buildPartialStringResponse( token, result );
        }
        else
        {
            return response().buildFinalStringResponse( result );
        }
    }

    private RemoteResponse<IterableSpecification<String>> types( int token,
        SimpleIterator<String> iterator )
    {
        return strings( token, iterator, server.typesBatchSize( iterator
            .count() ) );
    }

    private RemoteResponse<IterableSpecification<String>> keys( int token,
        SimpleIterator<String> iterator )
    {
        return strings( token, iterator, server
            .keysBatchSize( iterator.count() ) );
    }

    private static <E> List<E> consume( int token,
        Map<Integer, SimpleIterator<E>> map, SimpleIterator<E> iterator,
        int batchSize )
    {
        List<E> result = new ArrayList<E>( batchSize );
        int size = 0;
        for ( ; iterator.hasNext() && size < batchSize; size++ )
        {
            result.add( iterator.next() );
        }
        if ( iterator.hasNext() )
        {
            map.put( token, iterator );
        }
        return result;
    }

    // Communication

    RemoteResponse<EncodedObject> invokeServiceMethod( int serviceId,
        int functionIndex, EncodedObject[] arguments )
    {
        // TODO Auto-generated method stub
        return null;
    }

    RemoteResponse<EncodedObject> invokeObjectMethod( int serviceId,
        int objectId, int functionIndex, EncodedObject[] arguments )
    {
        // TODO Auto-generated method stub
        return null;
    }

    RemoteResponse<NodeSpecification> createNode()
    {
        resume();
        long id;
        try
        {
            id = server.createNode( connection.neo );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildNodeResponse( id );
    }

    RemoteResponse<Boolean> hasNodeWithId( long nodeId )
    {
        resume();
        boolean value;
        try
        {
            value = server.hasNodeWithId( connection.neo, nodeId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildBooleanResponse( value );
    }

    RemoteResponse<RelationshipSpecification> createRelationship(
        String relationshipTypeName, long startNodeId, long endNodeId )
    {
        resume();
        long id;
        try
        {
            id = server.createRelationship( connection.neo,
                relationshipTypeName, startNodeId, endNodeId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildRelationshipResponse( id, relationshipTypeName,
            startNodeId, endNodeId );
    }

    RemoteResponse<RelationshipSpecification> getRelationshipById(
        long relationshipId )
    {
        resume();
        RelationshipSpecification spec;
        try
        {
            spec = server.getRelationshipById( connection.neo, relationshipId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildRelationshipResponse( spec.relationshipId,
            spec.name, spec.startNodeId, spec.endNodeId );
    }

    RemoteResponse<Void> deleteNode( long nodeId )
    {
        resume();
        try
        {
            server.deleteNode( connection.neo, nodeId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildVoidResponse();
    }

    RemoteResponse<Void> deleteRelationship( long relationshipId )
    {
        resume();
        try
        {
            server.deleteRelationship( connection.neo, relationshipId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildVoidResponse();
    }

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        long nodeId, Direction direction )
    {
        resume();
        SimpleIterator<RelationshipSpecification> iterator;
        try
        {
            iterator = server.getAllRelationships( connection.neo, nodeId,
                direction );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return relationships( relTokenPool++, iterator );
    }

    RemoteResponse<IterableSpecification<EncodedObject>> getMoreObjects(
        int requestToken )
    {
        resume();
        SimpleIterator<EncodedObject> objects;
        try
        {
            objects = getObjects( requestToken );
        }
        catch ( Exception ex )
        {
            return response().buildErrorResponse( ex );
        }
        return objects( requestToken, objects );
    }

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int requestToken )
    {
        resume();
        SimpleIterator<RelationshipSpecification> relationships;
        try
        {
            relationships = getRelationships( requestToken );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return relationships( requestToken, relationships );
    }

    RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int requestToken )
    {
        resume();
        SimpleIterator<NodeSpecification> nodes;
        try
        {
            nodes = getNodes( requestToken );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return nodes( requestToken, nodes );
    }

    RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int requestToken )
    {
        resume();
        SimpleIterator<String> strings;
        try
        {
            strings = getStrings( requestToken );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return keys( requestToken, strings );
    }

    RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int requestToken )
    {
        resume();
        SimpleIterator<String> strings;
        try
        {
            strings = getStrings( requestToken );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return types( requestToken, strings );
    }

    RemoteResponse<Object> getNodeProperty( long nodeId, String key )
    {
        resume();
        Object value;
        try
        {
            value = server.getNodeProperty( connection.neo, nodeId, key );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildPropertyResponse( value );
    }

    RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        long nodeId )
    {
        resume();
        SimpleIterator<String> strings;
        try
        {
            strings = server.getNodePropertyKeys( connection.neo, nodeId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return keys( strTokenPool++, strings );
    }

    RemoteResponse<NodeSpecification> getReferenceNode()
    {
        resume();
        long id;
        try
        {
            id = server.getReferenceNode( connection.neo );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildNodeResponse( id );
    }

    RemoteResponse<Object> getRelationshipProperty( long relationshipId,
        String key )
    {
        resume();
        Object value;
        try
        {
            value = server.getRelationshipProperty( connection.neo,
                relationshipId, key );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildPropertyResponse( value );
    }

    RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        long relationshipId )
    {
        resume();
        SimpleIterator<String> strings;
        try
        {
            strings = server.getRelationshipPropertyKeys( connection.neo,
                relationshipId );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return keys( strTokenPool++, strings );
    }

    RemoteResponse<IterableSpecification<String>> getRelationshipTypes()
    {
        resume();
        SimpleIterator<String> strings;
        try
        {
            strings = server.getRelationshipTypes( connection.neo );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return types( strTokenPool++, strings );
    }

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        long nodeId, Direction direction, String[] relationshipTypeNames )
    {
        resume();
        SimpleIterator<RelationshipSpecification> relationships;
        try
        {
            relationships = server.getRelationships( connection.neo, nodeId,
                direction, relationshipTypeNames );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return relationships( relTokenPool++, relationships );
    }

    RemoteResponse<Boolean> hasNodeProperty( long nodeId, String key )
    {
        resume();
        boolean value;
        try
        {
            value = server.hasNodeProperty( connection.neo, nodeId, key );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildBooleanResponse( value );
    }

    RemoteResponse<Boolean> hasRelationshipProperty( long relationshiId,
        String key )
    {
        resume();
        boolean value;
        try
        {
            value = server.hasRelationshipProperty( connection.neo,
                relationshiId, key );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildBooleanResponse( value );
    }

    RemoteResponse<Object> removeNodeProperty( long nodeId, String key )
    {
        resume();
        Object value;
        try
        {
            value = server.removeNodeProperty( connection.neo, nodeId, key );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildPropertyResponse( value );
    }

    RemoteResponse<Object> removeRelationshipProperty( long relationshipId,
        String key )
    {
        resume();
        Object value;
        try
        {
            value = server.removeRelationshipProperty( connection.neo,
                relationshipId, key );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildPropertyResponse( value );
    }

    RemoteResponse<Object> setNodeProperty( long nodeId, String key,
        Object value )
    {
        resume();
        Object old;
        try
        {
            old = server.setNodeProperty( connection.neo, nodeId, key, value );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildPropertyResponse( old );
    }

    RemoteResponse<Object> setRelationshipProperty( long relationshipId,
        String key, Object value )
    {
        resume();
        Object old;
        try
        {
            old = server.setRelationshipProperty( connection.neo,
                relationshipId, key, value );
        }
        catch ( RuntimeException ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildPropertyResponse( old );
    }

    // indexing

    RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        Object service, String key, Object value )
    {
        resume();
        SimpleIterator<NodeSpecification> nodes;
        try
        {
            nodes = server.getIndexNodes( connection.neo, service, key, value );
        }
        catch ( Exception ex )
        {
            return response().buildErrorResponse( ex );
        }
        return nodes( nodTokenPool++, nodes );
    }

    RemoteResponse<Void> indexNode( Object service, long nodeId, String key,
        Object value )
    {
        resume();
        SimpleIterator<NodeSpecification> nodes;
        try
        {
            server.indexNode( connection.neo, service, nodeId, key, value );
        }
        catch ( Exception ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildVoidResponse();
    }

    RemoteResponse<Void> removeIndexNode( Object service, long nodeId,
        String key, Object value )
    {
        resume();
        SimpleIterator<NodeSpecification> nodes;
        try
        {
            server
                .removeIndexNode( connection.neo, service, nodeId, key, value );
        }
        catch ( Exception ex )
        {
            return response().buildErrorResponse( ex );
        }
        return response().buildVoidResponse();
    }
}
