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
package org.neo4j.remote.sites;

import java.rmi.RemoteException;

import org.neo4j.api.core.Direction;
import org.neo4j.remote.ClientConfigurator;
import org.neo4j.remote.Configuration;
import org.neo4j.remote.IterableSpecification;
import org.neo4j.remote.NodeSpecification;
import org.neo4j.remote.RelationshipSpecification;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteResponse;

class RmiConnectionAdapter implements RemoteConnection
{
    private RmiConnection rmi;

    RmiConnectionAdapter( RmiConnection rmi )
    {
        this.rmi = rmi;
    }

    private RmiConnection rmi()
    {
        if ( rmi != null )
        {
            return rmi;
        }
        else
        {
            throw new IllegalStateException( "Connection has been closed!" );
        }
    }

    private RuntimeException remoteException( RemoteException ex )
    {
        return new RuntimeException(
            "TODO: better exception type. RMI Connection failed.", ex );
    }

    public synchronized void close()
    {
        if ( rmi != null )
        {
            try
            {
                rmi.close();
                rmi = null;
            }
            catch ( RemoteException ex )
            {
                throw remoteException( ex );
            }
        }
    }

    public ClientConfigurator configure( Configuration config )
    {
        try
        {
            return rmi().configure( config );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public int beginTransaction()
    {
        try
        {
            return rmi().beginTransaction();
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public void commit( int transactionId )
    {
        try
        {
            rmi().commit( transactionId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public void rollback( int transactionId )
    {
        try
        {
            rmi().rollback( transactionId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
        int transactionId )
    {
        try
        {
            return rmi().getRelationshipTypes( transactionId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken )
    {
        try
        {
            return rmi().getMoreRelationshipTypes( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> closeRelationshipTypeIterator(
        int transactionId, int requestToken )
    {
        try
        {
            return rmi().closeRelationshipTypeIterator( transactionId,
                requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getAllNodes(
        int transactionId )
    {
        try
        {
            return rmi().getAllNodes( transactionId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<NodeSpecification> createNode( int transactionId )
    {
        try
        {
            return rmi().createNode( transactionId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<NodeSpecification> getReferenceNode( int transactionId )
    {
        try
        {
            return rmi().getReferenceNode( transactionId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId )
    {
        try
        {
            return rmi().hasNodeWithId( transactionId, nodeId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> deleteNode( int transactionId, long nodeId )
    {
        try
        {
            return rmi().deleteNode( transactionId, nodeId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken )
    {
        try
        {
            return rmi().getMoreNodes( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> closeNodeIterator( int transactionId,
        int requestToken )
    {
        try
        {
            return rmi().closeNodeIterator( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<RelationshipSpecification> createRelationship(
        int transactionId, String relationshipTypeName, long startNodeId,
        long endNodeId )
    {
        try
        {
            return rmi().createRelationship( transactionId,
                relationshipTypeName, startNodeId, endNodeId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<RelationshipSpecification> getRelationshipById(
        int transactionId, long relationshipId )
    {
        try
        {
            return rmi().getRelationshipById( transactionId, relationshipId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        int transactionId, long nodeId, Direction direction )
    {
        try
        {
            return rmi().getAllRelationships( transactionId, nodeId, direction );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        int transactionId, long nodeId, Direction direction,
        String[] relationshipTypeNames )
    {
        try
        {
            return rmi().getRelationships( transactionId, nodeId, direction,
                relationshipTypeNames );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken )
    {
        try
        {
            return rmi().getMoreRelationships( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> closeRelationshipIterator( int transactionId,
        int requestToken )
    {
        try
        {
            return rmi()
                .closeRelationshipIterator( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> deleteRelationship( int transactionId,
        long relationshipId )
    {
        try
        {
            return rmi().deleteRelationship( transactionId, relationshipId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Object> getNodeProperty( int transactionId,
        long nodeId, String key )
    {
        try
        {
            return rmi().getNodeProperty( transactionId, nodeId, key );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Object> getRelationshipProperty( int transactionId,
        long relationshipId, String key )
    {
        try
        {
            return rmi().getRelationshipProperty( transactionId,
                relationshipId, key );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Object> setNodeProperty( int transactionId,
        long nodeId, String key, Object value )
    {
        try
        {
            return rmi().setNodeProperty( transactionId, nodeId, key, value );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Object> setRelationshipProperty( int transactionId,
        long relationshipId, String key, Object value )
    {
        try
        {
            return rmi().setRelationshipProperty( transactionId,
                relationshipId, key, value );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        int transactionId, long nodeId )
    {
        try
        {
            return rmi().getNodePropertyKeys( transactionId, nodeId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        int transactionId, long relationshipId )
    {
        try
        {
            return rmi().getRelationshipPropertyKeys( transactionId,
                relationshipId );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int transactionId, int requestToken )
    {
        try
        {
            return rmi().getMorePropertyKeys( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> closePropertyKeyIterator( int transactionId,
        int requestToken )
    {
        try
        {
            return rmi().closePropertyKeyIterator( transactionId, requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Boolean> hasNodeProperty( int transactionId,
        long nodeId, String key )
    {
        try
        {
            return rmi().hasNodeProperty( transactionId, nodeId, key );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
        long relationshiId, String key )
    {
        try
        {
            return rmi().hasRelationshipProperty( transactionId, relationshiId,
                key );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Object> removeNodeProperty( int transactionId,
        long nodeId, String key )
    {
        try
        {
            return rmi().removeNodeProperty( transactionId, nodeId, key );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Object> removeRelationshipProperty(
        int transactionId, long relationshipId, String key )
    {
        try
        {
            return rmi().removeRelationshipProperty( transactionId,
                relationshipId, key );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Integer> getIndexServiceId( String indexName )
    {
        try
        {
            return rmi().getIndexId( indexName );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int indexId, String key, Object value )
    {
        try
        {
            return rmi().getIndexNodes( transactionId, indexId, key, value );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> indexNode( int transactionId, int indexId,
        long nodeId, String key, Object value )
    {
        try
        {
            return rmi().indexNode( transactionId, indexId, nodeId, key, value );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
        int indexId, long nodeId, String key, Object value )
    {
        try
        {
            return rmi().removeIndexNode( transactionId, indexId, nodeId, key,
                value );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }
}
