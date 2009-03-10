/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
 * Defines the operations in Neo that are relationship related.
 */
public interface RelationshipEventConsumer
{
    /**
     * Creates a relationship.
     * 
     * @param id
     *            The id of the relationship
     * @param directed
     *            Set to <CODE>true</CODE> if relationship is directed
     * @param firstNode
     *            The first node connected
     * @param secondNode
     *            The second node connected
     * @param type
     *            The id of the relationship type
     * @throws IOException
     *             If unable to create the relationship
     */
    public void createRelationship( int id, int firstNode, int secondNode,
        int type );

    /**
     * Deletes relationship with the given id.
     * 
     * @param id
     *            The id of the relationship
     * @throws IOException
     *             If unable to delete the relationship
     */
    public void deleteRelationship( int id );

    /**
     * Adds a property to the relationship.
     * 
     * @param relId
     *            The id of the relationship to add the property to
     * @param propertyId
     *            The id of the property
     * @param key
     *            The key of the property
     * @param value
     *            The value of the property
     * @throws IOException
     *             If unable to add property
     */
    public void addProperty( int relId, int propertyId, PropertyIndex index,
        Object value );

    /**
     * Changes the value of a property on a relationship.
     * 
     * @param propertyId
     *            The id of the property
     * @param value
     *            The new value
     * @throws IOException
     *             If unable to change property
     */
    public void changeProperty( int relId, int propertyId, Object value );

    /**
     * Removed a property from a relationship.
     * 
     * @param relId
     *            The id of the relationship
     * @param propertyId
     *            The id of the property
     * @throws IOException
     *             If unable to remove property
     */
    public void removeProperty( int relId, int propertyId );

    /**
     * Returns all properties connected to a relationship.
     * 
     * @param relId
     *            The id of the relationship
     * @return An array containing all properties connected to the relationship
     * @throws IOException
     *             If unable to get the properties
     */
    public ArrayMap<Integer,PropertyData> getProperties( int relId );

    /**
     * Gets a relationship with a given id.
     * 
     * @param id
     *            The id of the relationship
     * @return The relationship data
     * @throws IOException
     *             if unable to get the relationship
     */
    public RelationshipData getRelationship( int id );
}