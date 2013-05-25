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

import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.properties.Property;

public interface EntityWriteOperations
{
    // Currently, of course, most relevant operations here are still in the old core API implementation.

    void nodeDelete( long nodeId );

    void relationshipDelete( long relationshipId );

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link KeyWriteOperations#labelGetOrCreateForName(String)} or {@link
     * KeyReadOperations#labelGetForName(String)}.
     */
    boolean nodeAddLabel( long nodeId, long labelId ) throws EntityNotFoundException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link KeyWriteOperations#labelGetOrCreateForName(String)} or {@link
     * KeyReadOperations#labelGetForName(String)}.
     */
    boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException;

    Property nodeSetProperty( long nodeId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    Property relationshipSetProperty( long relationshipId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Property nodeRemoveProperty( long nodeId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    Property relationshipRemoveProperty( long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;
}
