/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.register.Register.DoubleLongRegister;

/** The indexing services view of the universe. */
public interface IndexStoreView extends PropertyAccessor
{
    /**
     * Retrieve all nodes in the database with a given label and property, as pairs of node id and property value.
     *
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodesWithPropertyAndLabel(
            IndexDescriptor descriptor, Visitor<NodePropertyUpdate, FAILURE> visitor );

    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids.
     *
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds, int[] propertyKeyIds,
            Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor,
            Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor );

    Iterable<NodePropertyUpdate> nodeAsUpdates( long nodeId );

    DoubleLongRegister indexUpdatesAndSize( IndexDescriptor descriptor, DoubleLongRegister output );

    DoubleLongRegister indexSample( IndexDescriptor descriptor, DoubleLongRegister output );

    void replaceIndexCounts( IndexDescriptor descriptor, long uniqueElements, long maxUniqueElements, long indexSize );

    void incrementIndexUpdates( IndexDescriptor descriptor, long updatesDelta );
}
