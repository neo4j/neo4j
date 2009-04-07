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
package org.neo4j.remote.sites;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

import org.neo4j.api.core.Direction;
import org.neo4j.remote.AsynchronousCallback;
import org.neo4j.remote.ClientConfigurator;
import org.neo4j.remote.Configuration;
import org.neo4j.remote.EncodedObject;
import org.neo4j.remote.IterableSpecification;
import org.neo4j.remote.NodeSpecification;
import org.neo4j.remote.RelationshipSpecification;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteResponse;
import org.neo4j.remote.ServiceSpecification;
import org.neo4j.remote.SynchronousCallback;

class RmiConnectionAdapter implements RemoteConnection
{
    private static class Callback extends UnicastRemoteObject implements
        RmiCallback
    {
        private static final long serialVersionUID = 1L;

        protected Callback( AsynchronousCallback callback )
            throws RemoteException
        {
            super();
        }

        public Callback( SynchronousCallback callback ) throws RemoteException
        {
            super();
        }
    }

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

    public ClientConfigurator configure( Configuration config,
        AsynchronousCallback callback )
    {
        try
        {
            return rmi().configure( config,
                callback != null ? new Callback( callback ) : null );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<Iterable<ServiceSpecification>> getServices(
        String interfaceName )
    {
        try
        {
            return rmi().getServices( interfaceName );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<EncodedObject> invokeServiceMethod(
        SynchronousCallback callback, int serviceId, int functionIndex,
        EncodedObject[] arguments )
    {
        try
        {
            return rmi().invokeServiceMethod( new Callback( callback ),
                serviceId, functionIndex, arguments );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<EncodedObject> invokeObjectMethod(
        SynchronousCallback callback, int serviceId, int objectId,
        int functionIndex, EncodedObject[] arguments )
    {
        try
        {
            return rmi().invokeObjectMethod( new Callback( callback ),
                serviceId, objectId, functionIndex, arguments );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<EncodedObject> invokeTransactionalServiceMethod(
        int transactionId, SynchronousCallback callback, int serviceId,
        int functionIndex, EncodedObject[] arguments )
    {
        try
        {
            return rmi().invokeTransactionalServiceMethod( transactionId,
                new Callback( callback ), serviceId, functionIndex, arguments );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<EncodedObject> invokeTransactionalObjectMethod(
        int transactionId, SynchronousCallback callback, int serviceId,
        int objectId, int functionIndex, EncodedObject[] arguments )
    {
        try
        {
            return rmi().invokeTransactionalObjectMethod( transactionId,
                new Callback( callback ), serviceId, objectId, functionIndex,
                arguments );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public void finalizeObject( int serviceId, int objectId )
    {
        try
        {
            rmi().finalizeObject( serviceId, objectId );
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

    public RemoteResponse<IterableSpecification<EncodedObject>> getMoreObjects(
        int requestToken )
    {
        try
        {
            return rmi().getMoreObjects( requestToken );
        }
        catch ( RemoteException ex )
        {
            throw remoteException( ex );
        }
    }

    public RemoteResponse<IterableSpecification<EncodedObject>> getMoreObjects(
        int transactionId, int requestToken )
    {
        try
        {
            return rmi().getTransactionalMoreObjects( transactionId,
                requestToken );
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
}
