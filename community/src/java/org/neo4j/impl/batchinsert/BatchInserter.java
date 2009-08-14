/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.batchinsert;

import java.util.Map;

import org.neo4j.api.core.RelationshipType;

/**
 * The batch inserter drops support for transactions and concurrency in favor 
 * of insertion speed. When done using the batch inserter {@link #shutdown()} 
 * must be invoked and complete succesfully for the Neo4j store to be in 
 * consistent state.  
 * <p>
 * Only one thread at a time may work against the batch inserter, multiple 
 * threads peforming concurrent access has to be synchronized.
 * <p>
 * Transactions are not supported so if your JVM/machine crash or you fail to 
 * invoke {@link #shutdown()} before JVM exits the Neo4j store can be considered 
 * beeing in non consistent state and the insertion has to be re-done from 
 * scratch.
 */
public interface BatchInserter
{
    /**
     * Creates a node assigning next available id to id and also adds any 
     * properties supplied.
     * 
     * @param properties a map containting properties or <code>null</code> if no 
     * properties should be added.
     * 
     * @return The id of the created node.
     */
    public long createNode( Map<String,Object> properties );
    
    /**
     * Checks if a node with the given id exists.
     * 
     * @param nodeId the id of the node.
     * @return <code>true</code> if the node exists.
     */
    public boolean nodeExists( long nodeId );
    
    /**
     * Sets the properties of a node. This method will remove any properties 
     * already existing and replace it with properties in the supplied map.
     * <p>
     * For best performance try supply all the nodes properties upon creation 
     * of the node. This method will delete any existing properties so using it 
     * together with {@link #getNodeProperties(long)} will have bad performance.
     * 
     * @param node the id of the node.
     * @param properties map containing the properties or <code>null</code> to 
     * clear all properties.
     */
    public void setNodeProperties( long node, Map<String,Object> properties );
    
    /**
     * Returns a map containing all the properties of this node.
     * 
     * @param nodeId the id of the node.
     * 
     * @return map contining this node's properties.
     */
    public Map<String,Object> getNodeProperties( long nodeId );
    
    /**
     * Returns an iterable over all the relationship ids connected to node with 
     * supplied id. 
     * 
     * @param nodeId the id of the node.
     * @return and iterable over the relationship ids connected to the node.
     */
    public Iterable<Long> getRelationshipIds( long nodeId );
    
    /**
     * Returns an iterable of {@link SimpleRelationship relationships} connected
     * to the node with supplied id.
     *  
     * @param nodeId the id of the node.
     * @return iterable over the relationships connected to the node.
     */
    public Iterable<SimpleRelationship> getRelationships( long nodeId );
    
    /**
     * Creates a node with supplied id and properties. If a node with the given 
     * id exist a runtime exception will be thrown.
     *  
     * @param id the id of the node to create.
     * @param properties map containting properties or <code>null</code> if no 
     * properties should be added.
     */
    public void createNode( long id, Map<String,Object> properties );

    /**
     * Creates a relationship between two nodes of a specific type. 
     * 
     * @param node1 the start node.
     * @param node2 the end node.
     * @param type relationship type. 
     * @param properties map containting properties or <code>null</code> if no 
     * properties should be added.
     * @return the id of the created relationship.
     */
    public long createRelationship( long node1, long node2, RelationshipType
        type, Map<String,Object> properties );
    
    /**
     * Gets a relationship by id.
     * 
     * @param relId the relationship id.
     * @return a simple relationship wrapper for the relationship.
     */
    public SimpleRelationship getRelatoinshipById( long relId );
    
    /**
     * Sets the properties of a relationship. This method will remove any 
     * properties already existing and replace it with properties in the 
     * supplied map.
     * <p>
     * For best performance try supply all the relationship properties upon 
     * creation of the relationship. This method will delete any existing 
     * properties so using it together with 
     * {@link #getRelationshipProperties(long)} will have bad performance.
     * 
     * @param rel the id of the relationship.
     * @param properties map containing the properties or <code>null</code> to 
     * clear all properties.
     */
    public void setRelationshipProperties( long rel, 
        Map<String,Object> properties );
    
    /**
     * Returns a map containing all the properties of the relationships.
     * 
     * @param relId the id of the relationship.
     * @return map contining the relationship's properties.
     */
    public Map getRelationshipProperties( long relId );

    /**
     * Shuts down this batch inserter syncing all changes that are still only 
     * in memory to disk. Failing to invoke this method may leave the Neo4j 
     * store in a inconsistent state.
     * <p>
     * After this method has been invoked any other method call to this batch 
     * inserter is illegal.
     */
    public void shutdown();

    /**
     * Returns the path to this Neo4j store.
     * 
     * @return the path to this Neo4j store.
     */
    public String getStore();
    
    /**
     * Returns the reference node id or <code>-1</code> if it doesn't exist.
     * 
     * @return the reference node
     */
    public long getReferenceNode();
        
}