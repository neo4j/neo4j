/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.nioneo.xa;

import java.io.IOException;

import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;
import org.neo4j.kernel.impl.util.ArrayMap;
import org.neo4j.kernel.impl.util.RelIdArray;

/**
 * Defines the operations in Neo4j kernel that are node related.
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
    public void createNode( long nodeId );

    /**
     * Deletes a node. The <CODE>nodeId</CODE> is the position of the record
     * where the node exist and will be deleted.
     * 
     * @param nodeId
     *            The id of the node
     * @throws IOException
     *             If
     */
    public ArrayMap<Integer,PropertyData> deleteNode( long nodeId );

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
    public boolean loadLightNode( long nodeId );

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
    public void addProperty( long nodeId, long propertyId, PropertyIndex index,
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
    public void changeProperty( long nodeId, long propertyId, Object value );

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
    public void removeProperty( long nodeId, long propertyId );

    /**
     * Returns all properties connected to a node.
     * 
     * @param nodeId
     *            The id of the node
     * @return An array containing all properties connected to the node
     * @throws IOException
     *             If unable to get the properties
     */
    public ArrayMap<Integer,PropertyData> getProperties( long nodeId,
            boolean light );

    public RelIdArray getCreatedNodes();

    public boolean isNodeCreated( long nodeId );
}