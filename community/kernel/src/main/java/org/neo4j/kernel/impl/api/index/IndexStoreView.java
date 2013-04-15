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
package org.neo4j.kernel.impl.api.index;

import java.util.Iterator;

import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;

/** The indexing services view of the universe. */
public interface IndexStoreView
{
    /**
     * Get properties of a node, if those properties exist.
     */
    Iterator<Pair<Integer, Object>> getNodeProperties( long nodeId, Iterator<Long> propertyKeys );

    /**
     * Retrieve all nodes in the database with a given label and property, as pairs of node id and property value.
     *
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    StoreScan visitNodesWithPropertyAndLabel( IndexDescriptor descriptor, Visitor<NodePropertyUpdate> visitor );

    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids.
     *
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    StoreScan visitNodes( long[] labelIds, long[] propertyKeyIds, Visitor<NodePropertyUpdate> visitor );
}
