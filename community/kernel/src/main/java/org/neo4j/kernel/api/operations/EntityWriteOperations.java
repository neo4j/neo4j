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

import org.neo4j.kernel.api.EntityNotFoundException;
import org.neo4j.kernel.api.PropertyKeyIdNotFoundException;

public interface EntityWriteOperations
{
    // Currently, of course, most relevant operations here are still in the old core API implementation.

    void deleteNode( long nodeId );

    /**
     * Labels a node with the label corresponding to the given label id.
     * If the node already had that label nothing will happen. Label ids
     * are retrieved from {@link KeyWriteOperations#getOrCreateLabelId(String)} or {@link
     * KeyReadOperations#getLabelId(String)}.
     */
    boolean addLabelToNode( long labelId, long nodeId ) throws EntityNotFoundException;

    /**
     * Removes a label with the corresponding id from a node.
     * If the node doesn't have that label nothing will happen. Label ids
     * are retrieved from {@link KeyWriteOperations#getOrCreateLabelId(String)} or {@link
     * KeyReadOperations#getLabelId(String)}.
     */
    boolean removeLabelFromNode( long labelId, long nodeId ) throws EntityNotFoundException;

    /** Set a node's property given the node's id, the property key id, and the value */
    void nodeSetPropertyValue( long nodeId, long propertyId, Object value )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;

    /**
     * Remove a node's property given the node's id and the property key id and return the value to which
     * it was set or null if it was not set on the node
     */
    Object nodeRemoveProperty( long nodeId, long propertyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException;
}
