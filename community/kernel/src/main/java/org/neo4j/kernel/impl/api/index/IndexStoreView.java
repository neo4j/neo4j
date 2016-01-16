/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Collection;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.register.Register.DoubleLongRegister;

/** The indexing services view of the universe. */
public interface IndexStoreView extends PropertyAccessor
{
    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids. This scan additionally accepts a visitor
     * for label updates for a joint scan.
     *
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds, int[] propertyKeyIds,
            Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor,
            Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor );

    /**
     * Retrieve all nodes in the database which has got one or more of the given labels AND
     * one or more of the given property key ids.
     *
     * @return a {@link StoreScan} to start and to stop the scan.
     */
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds, int[] propertyKeyIds,
            Visitor<NodePropertyUpdate, FAILURE> propertyUpdateVisitor );

    /**
     * Produces {@link NodePropertyUpdate} objects from reading node {@code nodeId}, its labels and properties
     * and puts those updates into {@code target}.
     *
     * @param nodeId id of node to load.
     * @param reusableRecord {@link NodeRecord} to load node record into.
     * @param target {@link Collection} to add updates into.
     */
    void nodeAsUpdates( long nodeId, NodeRecord reusableRecord, Collection<NodePropertyUpdate> target );

    DoubleLongRegister indexUpdatesAndSize( IndexDescriptor descriptor, DoubleLongRegister output );

    DoubleLongRegister indexSample( IndexDescriptor descriptor, DoubleLongRegister output );

    void replaceIndexCounts( IndexDescriptor descriptor, long uniqueElements, long maxUniqueElements, long indexSize );

    void incrementIndexUpdates( IndexDescriptor descriptor, long updatesDelta );
}
