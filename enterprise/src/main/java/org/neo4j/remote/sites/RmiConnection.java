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

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.neo4j.api.core.Direction;
import org.neo4j.remote.ClientConfigurator;
import org.neo4j.remote.Configuration;
import org.neo4j.remote.IterableSpecification;
import org.neo4j.remote.NodeSpecification;
import org.neo4j.remote.RelationshipSpecification;
import org.neo4j.remote.RemoteResponse;

interface RmiConnection extends Remote
{
    void close() throws RemoteException;

    ClientConfigurator configure( Configuration config ) throws RemoteException;

    int beginTransaction() throws RemoteException;

    void commit( int transactionId ) throws RemoteException;

    void rollback( int transactionId ) throws RemoteException;

    RemoteResponse<IterableSpecification<String>> getRelationshipTypes(
        int transactionId ) throws RemoteException;

    RemoteResponse<IterableSpecification<String>> getMoreRelationshipTypes(
        int transactionId, int requestToken ) throws RemoteException;

    RemoteResponse<Void> closeRelationshipTypeIterator( int transactionId,
        int requestToken ) throws RemoteException;

    RemoteResponse<NodeSpecification> createNode( int transactionId )
        throws RemoteException;

    RemoteResponse<NodeSpecification> getReferenceNode( int transactionId )
        throws RemoteException;

    RemoteResponse<Boolean> hasNodeWithId( int transactionId, long nodeId )
        throws RemoteException;

    RemoteResponse<Void> deleteNode( int transactionId, long nodeId )
        throws RemoteException;

    RemoteResponse<IterableSpecification<NodeSpecification>> getAllNodes(
        int transactionId ) throws RemoteException;

    RemoteResponse<IterableSpecification<NodeSpecification>> getMoreNodes(
        int transactionId, int requestToken ) throws RemoteException;

    RemoteResponse<Void> closeNodeIterator( int transactionId, int requestToken )
        throws RemoteException;

    RemoteResponse<RelationshipSpecification> createRelationship(
        int transactionId, String relationshipTypeName, long startNodeId,
        long endNodeId ) throws RemoteException;

    RemoteResponse<RelationshipSpecification> getRelationshipById(
        int transactionId, long relationshipId ) throws RemoteException;

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getAllRelationships(
        int transactionId, long nodeId, Direction direction )
        throws RemoteException;

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getRelationships(
        int transactionId, long nodeId, Direction direction,
        String[] relationshipTypeNames ) throws RemoteException;

    RemoteResponse<IterableSpecification<RelationshipSpecification>> getMoreRelationships(
        int transactionId, int requestToken ) throws RemoteException;

    RemoteResponse<Void> closeRelationshipIterator( int transactionId,
        int requestToken ) throws RemoteException;

    RemoteResponse<Void> deleteRelationship( int transactionId,
        long relationshipId ) throws RemoteException;

    RemoteResponse<Object> getNodeProperty( int transactionId, long nodeId,
        String key ) throws RemoteException;

    RemoteResponse<Object> getRelationshipProperty( int transactionId,
        long relationshipId, String key ) throws RemoteException;

    RemoteResponse<Object> setNodeProperty( int transactionId, long nodeId,
        String key, Object value ) throws RemoteException;

    RemoteResponse<Object> setRelationshipProperty( int transactionId,
        long relationshipId, String key, Object value ) throws RemoteException;

    RemoteResponse<IterableSpecification<String>> getNodePropertyKeys(
        int transactionId, long nodeId ) throws RemoteException;

    RemoteResponse<IterableSpecification<String>> getRelationshipPropertyKeys(
        int transactionId, long relationshipId ) throws RemoteException;

    RemoteResponse<IterableSpecification<String>> getMorePropertyKeys(
        int transactionId, int requestToken ) throws RemoteException;

    RemoteResponse<Void> closePropertyKeyIterator( int transactionId,
        int requestToken ) throws RemoteException;

    RemoteResponse<Boolean> hasNodeProperty( int transactionId, long nodeId,
        String key ) throws RemoteException;

    RemoteResponse<Boolean> hasRelationshipProperty( int transactionId,
        long relationshiId, String key ) throws RemoteException;

    RemoteResponse<Object> removeNodeProperty( int transactionId, long nodeId,
        String key ) throws RemoteException;

    RemoteResponse<Object> removeRelationshipProperty( int transactionId,
        long relationshipId, String key ) throws RemoteException;

    RemoteResponse<Integer> getIndexId( String indexName )
        throws RemoteException;

    RemoteResponse<IterableSpecification<NodeSpecification>> getIndexNodes(
        int transactionId, int indexId, String key, Object value )
        throws RemoteException;

    RemoteResponse<Void> indexNode( int transactionId, int indexId,
        long nodeId, String key, Object value ) throws RemoteException;

    RemoteResponse<Void> removeIndexNode( int transactionId, int indexId,
        long nodeId, String key, Object value ) throws RemoteException;
}
