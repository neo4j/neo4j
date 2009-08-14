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
package org.neo4j.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.impl.core.PropertyIndex;
import org.neo4j.impl.nioneo.store.PropertyData;
import org.neo4j.impl.nioneo.store.RelationshipData;
import org.neo4j.impl.util.ArrayMap;

/**
 * Defines the operations in Neo that are node related.
 */
public interface NodeEventConsumer
{
    /**
     * Creates a node. The <CODE>nodeId</CODE> is the position of the record
     * where the node will be created.
     * 
     * @param nodeId
     *            The id of the node
     * @throws IOException
     *             If unable to create or if node already exist
     */
    public void createNode( int nodeId );

    /**
     * Deletes a node. The <CODE>nodeId</CODE> is the position of the record
     * where the node exist and will be deleted.
     * 
     * @param nodeId
     *            The id of the node
     * @throws IOException
     *             If
     */
    public void deleteNode( int nodeId );

    /**
     * Checks if a node exists. If the record <CODE>nodeId</CODE> is in use
     * <CODE>true</CODE> is returned.
     * 
     * @param nodeId
     *            The id of the node
     * @return True if node exists
     * @throws IOException
     *             If unable to check for node
     */
    public boolean loadLightNode( int nodeId );

    /**
     * Adds a property to the node.
     * 
     * @param nodeId
     *            The id of the node to add the property to
     * @param propertyId
     *            The id of the property
     * @param key
     *            The key of the property
     * @param value
     *            The value of the property
     * @throws IOException
     *             If unable to add property
     */
    public void addProperty( int nodeId, int propertyId, PropertyIndex index,
        Object value );

    /**
     * Changes the value of a property on a node.
     * 
     * @param propertyId
     *            The id of the property
     * @param value
     *            The new value
     * @throws IOException
     *             If unable to change property
     */
    public void changeProperty( int nodeId, int propertyId, Object value );

    /**
     * Removed a property from a node.
     * 
     * @param nodeId
     *            The id of the node
     * @param propertyId
     *            The id of the property
     * @throws IOException
     *             If unable to remove property
     */
    public void removeProperty( int nodeId, int propertyId );

    /**
     * Returns all properties connected to a node.
     * 
     * @param nodeId
     *            The id of the node
     * @return An array containing all properties connected to the node
     * @throws IOException
     *             If unable to get the properties
     */
    public ArrayMap<Integer,PropertyData> getProperties( int nodeId );

    /**
     * Returns all relationships connected to the node.
     * 
     * @param nodeId
     *            The id of the node
     * @return An array containing all relationships connected to the node
     * @throws IOException
     *             If unable to get the relationships
     */
    public Iterable<RelationshipData> getRelationships( int nodeId );
}