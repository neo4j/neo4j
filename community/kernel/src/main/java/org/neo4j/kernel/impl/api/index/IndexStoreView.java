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
import java.util.function.IntPredicate;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.storageengine.api.schema.PopulationProgress;

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
    <FAILURE extends Exception> StoreScan<FAILURE> visitNodes(
            IntPredicate labelIdFilter, IntPredicate propertyKeyIdFilter,
            Visitor<NodePropertyUpdates, FAILURE> propertyUpdateVisitor,
            Visitor<NodeLabelUpdate, FAILURE> labelUpdateVisitor );

    /**
     * Produces {@link NodePropertyUpdate} objects from reading node {@code nodeId}, its labels and properties
     * and puts those updates into {@code target}.
     *
     * @param nodeId id of node to load.
     * @param target {@link Collection} to add updates into.
     */
    void nodeAsUpdates( long nodeId, Collection<NodePropertyUpdate> target );

    DoubleLongRegister indexUpdatesAndSize( IndexDescriptor descriptor, DoubleLongRegister output );

    DoubleLongRegister indexSample( IndexDescriptor descriptor, DoubleLongRegister output );

    void replaceIndexCounts( IndexDescriptor descriptor, long uniqueElements, long maxUniqueElements, long indexSize );

    void incrementIndexUpdates( IndexDescriptor descriptor, long updatesDelta );

    StoreScan EMPTY_SCAN = new StoreScan()
    {
        @Override
        public void run() throws Exception
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public PopulationProgress getProgress()
        {
            return PopulationProgress.DONE;
        }
    };

    IndexStoreView EMPTY = new IndexStoreView()
    {
        @Override
        public Property getProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
        {
            return Property.noNodeProperty( nodeId, propertyKeyId );
        }

        @Override
        public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( IntPredicate labelIdFilter,
                IntPredicate propertyKeyIdFilter, Visitor<NodePropertyUpdates,FAILURE> propertyUpdateVisitor,
                Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
        {
            return EMPTY_SCAN;
        }

        @Override
        public void replaceIndexCounts( IndexDescriptor descriptor, long uniqueElements, long maxUniqueElements,
                long indexSize )
        {
        }

        @Override
        public void nodeAsUpdates( long nodeId, Collection<NodePropertyUpdate> target )
        {
        }

        @Override
        public DoubleLongRegister indexUpdatesAndSize( IndexDescriptor descriptor, DoubleLongRegister output )
        {
            return output;
        }

        @Override
        public DoubleLongRegister indexSample( IndexDescriptor descriptor, DoubleLongRegister output )
        {
            return output;
        }

        @Override
        public void incrementIndexUpdates( IndexDescriptor descriptor, long updatesDelta )
        {
        }
    };
}
