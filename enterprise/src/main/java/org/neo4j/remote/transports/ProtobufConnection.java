/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

package org.neo4j.remote.transports;

import org.neo4j.graphdb.Direction;
import org.neo4j.remote.ClientConfigurator;
import org.neo4j.remote.Configuration;
import org.neo4j.remote.IterableSpecification;
import org.neo4j.remote.NodeSpecification;
import org.neo4j.remote.RelationshipSpecification;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteResponse;
import org.neo4j.remote.impl.protobuf.RemoteNeo;

import com.google.protobuf.InvalidProtocolBufferException;

class ProtobufConnection implements RemoteConnection
{
    private RemoteNeo.RemoteResponse send( RemoteNeo.RemoteRequest request )
    {
        // TODO: implement this properly
        try
        {
            return RemoteNeo.RemoteResponse.parseFrom( (byte[]) null );
        }
        catch ( InvalidProtocolBufferException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static RemoteNeo.RemoteRequest.Builder request()
    {
        return RemoteNeo.RemoteRequest.newBuilder();
    }

    public int beginTransaction()
    {
        return send(
                request().setMethod(
                        RemoteNeo.RemoteRequest.RequestMethod.BEGIN_TX ).build() ).getTxId();
    }

    public void close()
    {
        // TODO Auto-generated method stub

    }

    public RemoteResponse<Void> closeNodeIterator( int transactionId,
            int requestToken )
    {
        return voidResponse( request().setMethod(
                RemoteNeo.RemoteRequest.RequestMethod.BEGIN_TX ).setTxId(
                transactionId ).build() );
    }

    private RemoteResponse<Void> voidResponse( Object object )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> closePropertyKeyIterator( int transactionId,
            int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> closeRelationshipIterator( int transactionId,
            int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> closeRelationshipTypeIterator(
            int transactionId, int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void commit( int transactionId )
    {
        // TODO Auto-generated method stub

    }

    public ClientConfigurator configure( Configuration config )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<NodeSpecification> createNode( int transactionId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<RelationshipSpecification> createRelationship(
            int transactionId, String relationshipTypeName, long startNodeId,
            long endNodeId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> deleteNode( int transactionId, long nodeId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> deleteRelationship( int transactionId,
            long relationshipId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getAllNodes(
            int transactionId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
            int transactionId, long nodeId, Direction direction )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
            int transactionId, int indexId, String key, Object value )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Integer> getIndexServiceId( String indexName )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
            int transactionId, int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
            int transactionId, int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
            int transactionId, int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
            int transactionId, int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Object> getNodeProperty( int transactionId,
            long nodeId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
            int transactionId, long nodeId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<NodeSpecification> getReferenceNode( int transactionId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<RelationshipSpecification> getRelationshipById(
            int transactionId, long relationshipId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Object> getRelationshipProperty( int transactionId,
            long relationshipId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
            int transactionId, long relationshipId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
            int transactionId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
            int transactionId, long nodeId, Direction direction,
            String[] relationshipTypeNames )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Boolean> hasNodeProperty( int transactionId,
            long nodeId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
            long relationshipId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> indexNode( int transactionId, int indexId,
            long nodeId, String key, Object value )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
            int indexId, long nodeId, String key, Object value )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
            int indexId, long nodeId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
            int indexId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Object> removeNodeProperty( int transactionId,
            long nodeId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Object> removeRelationshipProperty(
            int transactionId, long relationshipId, String key )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public void rollback( int transactionId )
    {
        // TODO Auto-generated method stub

    }

    public RemoteResponse<Object> setNodeProperty( int transactionId,
            long nodeId, String key, Object value )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Object> setRelationshipProperty( int transactionId,
            long relationshipId, String key, Object value )
    {
        // TODO Auto-generated method stub
        return null;
    }
}
