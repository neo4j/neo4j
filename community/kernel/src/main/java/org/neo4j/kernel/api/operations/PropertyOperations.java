/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.api.operations;

import java.util.Iterator;

import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.PropertyNotFoundException;

public interface PropertyOperations
{

    /**
     * Returns a property key id for a property key. If the key doesn't exist prior to
     * this call it gets created.
     */
    long getOrCreatePropertyKeyId( String propertyKey ) throws ConstraintViolationKernelException;

    /**
     * Returns a property key id for the given property key. If the property key doesn't exist a
     * {@link org.neo4j.kernel.api.PropertyKeyNotFoundException} will be thrown.
     */
    long getPropertyKeyId( String propertyKey ) throws PropertyKeyNotFoundException;

    /**
     * Returns the name of a property given it's property key id
     */
    String getPropertyKeyName( long propertyKeyId ) throws PropertyKeyIdNotFoundException;

    /**
     * Returns the value of the property given it's property key id for the node with the given node id
     */
    Object getNodePropertyValue( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, PropertyNotFoundException, EntityNotFoundException;

    /**
     * Returns true if node has the property given it's property key id for the node with the given node id
     */
    boolean nodeHasProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    /**
     * Set a node's property given the node's id, the property key id, and the value
     */
    void nodeSetPropertyValue( long nodeId, long propertyKeyId, Object value )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Object nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    /**
     * Return all property keys associated with a node.
     */
    Iterator<Long> listNodePropertyKeys( long nodeId );

    /**
     * Return all property keys associated with a relationship.
     */
    Iterator<Long> listRelationshipPropertyKeys( long relationshipId );
}
