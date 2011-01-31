/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import org.neo4j.graphdb.Direction;
import org.neo4j.remote.BasicGraphDatabaseServer;
import org.neo4j.remote.ClientConfigurator;
import org.neo4j.remote.Configuration;
import org.neo4j.remote.IterableSpecification;
import org.neo4j.remote.NodeSpecification;
import org.neo4j.remote.RelationshipSpecification;
import org.neo4j.remote.RemoteConnection;
import org.neo4j.remote.RemoteResponse;
import org.neo4j.remote.ConnectionTarget;

class RmiConnectionServer extends UnicastRemoteObject implements RmiConnection
{
    private static final long serialVersionUID = 1L;

    private static class LoginServer extends UnicastRemoteObject implements
        RmiRemoteTarget
    {
        private static final long serialVersionUID = 1L;
        private final ConnectionTarget site;
        private int port = 0;
        private RMIClientSocketFactory client = null;
        private RMIServerSocketFactory server = null;

        LoginServer( ConnectionTarget site ) throws RemoteException
        {
            this.site = site;
        }

        LoginServer( ConnectionTarget site, int port ) throws RemoteException
        {
            super( port );
            this.port = port;
            this.site = site;
        }

        LoginServer( ConnectionTarget site, int port, RMIClientSocketFactory client,
            RMIServerSocketFactory server ) throws RemoteException
        {
            super( port, client, server );
            this.site = site;
            this.port = port;
            this.client = client;
            this.server = server;
        }

        public RmiConnection connect() throws RemoteException
        {
            return connection( site.connect() );
        }

        public RmiConnection connect( String username, String password )
            throws RemoteException
        {
            return connection( site.connect( username, password ) );
        }

        private RmiConnection connection( RemoteConnection connection )
            throws RemoteException
        {
            if ( client != null )
            {
                return new RmiConnectionServer( connection, port, client,
                    server );
            }
            else
            {
                return new RmiConnectionServer( connection, port );
            }
        }
    }

    private final transient RemoteConnection connection;

    static RmiRemoteTarget setup( BasicGraphDatabaseServer server ) throws RemoteException
    {
        return new LoginServer( server );
    }

    static RmiRemoteTarget setup( BasicGraphDatabaseServer server, int port )
        throws RemoteException
    {
        return new LoginServer( server, port );
    }

    static RmiRemoteTarget setup( BasicGraphDatabaseServer server, int port,
        RMIClientSocketFactory clientSocket, RMIServerSocketFactory serverSocket )
        throws RemoteException
    {
        return new LoginServer( server, port, clientSocket, serverSocket );
    }

    private RmiConnectionServer( RemoteConnection connection, int port )
        throws RemoteException
    {
        super( port );
        this.connection = connection;
    }

    private RmiConnectionServer( RemoteConnection connection, int port,
        RMIClientSocketFactory client, RMIServerSocketFactory server )
        throws RemoteException
    {
        super( port, client, server );
        this.connection = connection;
    }

    public ClientConfigurator configure( Configuration config )
    {
        return connection.configure( config );
    }

    public void close()
    {
        connection.close();
    }

    public int beginTransaction()
    {
        return connection.beginTransaction();
    }

    public void commit( int transactionId )
    {
        connection.commit( transactionId );
    }

    public void rollback( int transactionId )
    {
        connection.rollback( transactionId );
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
        int transactionId )
    {
        return connection.getRelationshipTypes( transactionId );
    }

    public RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken )
    {
        return connection
            .getMoreRelationshipTypes( transactionId, requestToken );
    }

    public RemoteResponse<Void> closeRelationshipTypeIterator(
        int transactionId, int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<NodeSpecification> createNode( int transactionId )
    {
        return connection.createNode( transactionId );
    }

    public RemoteResponse<NodeSpecification> getReferenceNode( int transactionId )
    {
        return connection.getReferenceNode( transactionId );
    }

    public RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId )
    {
        return connection.hasNodeWithId( transactionId, nodeId );
    }

    public RemoteResponse<Void> deleteNode( int transactionId, long nodeId )
    {
        return connection.deleteNode( transactionId, nodeId );
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getAllNodes(
        int transactionId )
    {
        return connection.getAllNodes( transactionId );
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken )
    {
        return connection.getMoreNodes( transactionId, requestToken );
    }

    public RemoteResponse<Void> closeNodeIterator( int transactionId,
        int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<RelationshipSpecification> createRelationship(
        int transactionId, String relationshipTypeName, long startNodeId,
        long endNodeId )
    {
        return connection.createRelationship( transactionId,
            relationshipTypeName, startNodeId, endNodeId );
    }

    public RemoteResponse<RelationshipSpecification> getRelationshipById(
        int transactionId, long relationshipId )
    {
        return connection.getRelationshipById( transactionId, relationshipId );
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        int transactionId, long nodeId, Direction direction )
    {
        return connection
            .getAllRelationships( transactionId, nodeId, direction );
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        int transactionId, long nodeId, Direction direction,
        String[] relationshipTypeNames )
    {
        return connection.getRelationships( transactionId, nodeId, direction,
            relationshipTypeNames );
    }

    public RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken )
    {
        return connection.getMoreRelationships( transactionId, requestToken );
    }

    public RemoteResponse<Void> closeRelationshipIterator( int transactionId,
        int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Void> deleteRelationship( int transactionId,
        long relationshipId )
    {
        return connection.deleteRelationship( transactionId, relationshipId );
    }

    public RemoteResponse<Object> getNodeProperty( int transactionId,
        long nodeId, String key )
    {
        return connection.getNodeProperty( transactionId, nodeId, key );
    }

    public RemoteResponse<Object> getRelationshipProperty( int transactionId,
        long relationshipId, String key )
    {
        return connection.getRelationshipProperty( transactionId,
            relationshipId, key );
    }

    public RemoteResponse<Object> setNodeProperty( int transactionId,
        long nodeId, String key, Object value )
    {
        return connection.setNodeProperty( transactionId, nodeId, key, value );
    }

    public RemoteResponse<Object> setRelationshipProperty( int transactionId,
        long relationshipId, String key, Object value )
    {
        return connection.setRelationshipProperty( transactionId,
            relationshipId, key, value );
    }

    public RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        int transactionId, long nodeId )
    {
        return connection.getNodePropertyKeys( transactionId, nodeId );
    }

    public RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        int transactionId, long relationshipId )
    {
        return connection.getRelationshipPropertyKeys( transactionId,
            relationshipId );
    }

    public RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int transactionId, int requestToken )
    {
        return connection.getMorePropertyKeys( transactionId, requestToken );
    }

    public RemoteResponse<Void> closePropertyKeyIterator( int transactionId,
        int requestToken )
    {
        // TODO Auto-generated method stub
        return null;
    }

    public RemoteResponse<Boolean> hasNodeProperty( int transactionId,
        long nodeId, String key )
    {
        return connection.hasNodeProperty( transactionId, nodeId, key );
    }

    public RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
        long relationshiId, String key )
    {
        return connection.hasRelationshipProperty( transactionId,
            relationshiId, key );
    }

    public RemoteResponse<Object> removeNodeProperty( int transactionId,
        long nodeId, String key )
    {
        return connection.removeNodeProperty( transactionId, nodeId, key );
    }

    public RemoteResponse<Object> removeRelationshipProperty(
        int transactionId, long relationshipId, String key )
    {
        return connection.removeRelationshipProperty( transactionId,
            relationshipId, key );
    }

    public RemoteResponse<Integer> getIndexId( String indexName )
        throws RemoteException
    {
        return connection.getIndexServiceId( indexName );
    }

    public RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int indexId, String key, Object value )
        throws RemoteException
    {
        return connection.getIndexNodes( transactionId, indexId, key, value );
    }

    public RemoteResponse<Void> indexNode( int transactionId, int indexId,
        long nodeId, String key, Object value ) throws RemoteException
    {
        return connection
            .indexNode( transactionId, indexId, nodeId, key, value );
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
        int indexId, long nodeId, String key, Object value )
        throws RemoteException
    {
        return connection.removeIndexNode( transactionId, indexId, nodeId, key,
            value );
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
        int indexId, long nodeId, String key )
        throws RemoteException
    {
        return connection.removeIndexNode( transactionId, indexId, nodeId, key );
    }

    public RemoteResponse<Void> removeIndexNode( int transactionId,
        int indexId, String key ) throws RemoteException
    {
        return connection.removeIndexNode( transactionId, indexId, key );
    }
}
