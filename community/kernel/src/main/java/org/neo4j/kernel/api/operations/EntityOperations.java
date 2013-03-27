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

import org.neo4j.kernel.api.index.IndexNotFoundKernelException;

/**
 * Node and Relationship creation, deletion and lookup.
 */
public interface EntityOperations
{

    // Currently, of course, most relevant operations here are still in the old core API implementation.

    /**
     * @param labelId the label id of the label that returned nodes are guaranteed to have
     * @return ids of all nodes that have the given label
     */
    Iterator<Long> getNodesWithLabel( long labelId );

    /**
     * Returns an iterable with the matched nodes.
     * @throws org.neo4j.kernel.api.index.IndexNotFoundKernelException if no such index found.
     */
    Iterator<Long> exactIndexLookup( long indexId, Object value ) throws IndexNotFoundKernelException;

}
