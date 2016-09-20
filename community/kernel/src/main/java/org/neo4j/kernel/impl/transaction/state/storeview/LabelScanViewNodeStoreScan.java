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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Store scan view that will try to minimize amount of scanned nodes by using label scan store {@link LabelScanStore}
 * as a source of known labeled node ids.
 * @param <FAILURE>
 */
public class LabelScanViewNodeStoreScan<FAILURE extends Exception> extends StoreViewNodeStoreScan<FAILURE>
{
    private NeoStoreIndexStoreView storeView;
    private final LabelScanStore labelScanStore;

    PrimitiveLongObjectMap<PrimitiveLongObjectMap<PrimitiveLongSet>> propertyLabelNodes = Primitive.longObjectMap();

    public LabelScanViewNodeStoreScan( NeoStoreIndexStoreView storeView, NodeStore nodeStore, LockService locks,
            PropertyStore propertyStore,
            LabelScanStore labelScanStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
            IntPredicate propertyKeyIdFilter )
    {
        super( nodeStore, locks, propertyStore, labelUpdateVisitor, propertyUpdatesVisitor, labelIds,
                propertyKeyIdFilter );
        this.storeView = storeView;
        this.labelScanStore = labelScanStore;
    }

    @Override
    public PrimitiveLongResourceIterator getNodeIdIterator()
    {
        return new LabelScanViewIdIterator( labelScanStore, labelIds );
    }

    @Override
    public void configure( List<MultipleIndexPopulator.IndexPopulation> populations )
    {
        populations.forEach( population -> population.populator.configureSampling( false ) );
    }

    @Override
    public void complete( IndexPopulator indexPopulator, IndexDescriptor descriptor )
            throws EntityNotFoundException, PropertyNotFoundException, IOException, IndexEntryConflictException
    {

        PrimitiveLongObjectMap<PrimitiveLongSet> labelNodes = propertyLabelNodes.get( descriptor.getPropertyKeyId() );
        if ( labelNodes != null )
        {
            int labelId = descriptor.getLabelId();
            PrimitiveLongSet nodes = labelNodes.get( labelId );
            if ( nodes != null )
            {
                long[] labels = new long[labelId];
                PrimitiveLongIterator nodeIterator = nodes.iterator();
                NodeRecord nodeRecord = new NodeRecord( -1 );
                try ( IndexUpdater updater = indexPopulator.newPopulatingUpdater( storeView ) )
                {
                    while ( nodeIterator.hasNext() )
                    {
                        long nodeId = nodeIterator.next();
                        NodeRecord record = nodeStore.getRecord( nodeId, nodeRecord, RecordLoad.FORCE );

                        int propertyKeyId = descriptor.getPropertyKeyId();

                        updater.process(
                                NodePropertyUpdate.remove( nodeId, propertyKeyId, StringUtils.EMPTY, labels ) );
                        if ( record.inUse() )
                        {
                            Property property = storeView.getProperty( nodeId, propertyKeyId );
                            if ( property.isDefined() )
                            {
                                Object propertyValue = property.value();
                                updater.process(
                                        NodePropertyUpdate.change( nodeId, propertyKeyId, StringUtils.EMPTY,
                                                labels, propertyValue, labels ) );
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Checks if provided node property update is applicable for current store view.
     * In case if we use label store view - updates are always applicable, otherwise fallback to default behaviour of
     * neo store view.
     *
     * @param updater updater that should process updates in required
     * @param update node property update
     * @param currentlyIndexedNodeId currently indexed node id
     */
    @Override
    public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, NodePropertyUpdate update,
            long currentlyIndexedNodeId )
    {
        long[] labels;
        switch ( update.getUpdateMode() )
        {
        case ADDED:
            labels = update.getLabelsAfter();
            break;
        case CHANGED:
            labels = Arrays.stream( update.getLabelsBefore() )
                    .filter( item -> Arrays.binarySearch( update.getLabelsAfter(), item ) >= 0 )
                    .toArray();
            break;
        case REMOVED:
            labels = update.getLabelsBefore();
            break;
        default:
            throw new UnsupportedOperationException( "unsupported update mode" );
        }
        int property = update.getPropertyKeyId();
        long nodeId = update.getNodeId();
        for ( long label : labels )
        {
            PrimitiveLongObjectMap<PrimitiveLongSet> labelsNodes = propertyLabelNodes.get( property );
            if ( labelsNodes == null)
            {
                labelsNodes = Primitive.longObjectMap();
                propertyLabelNodes.put( property, labelsNodes );
            }
            PrimitiveLongSet nodes = labelsNodes.get( label );
            if (nodes == null)
            {
                nodes = Primitive.longSet();
                labelsNodes.put( label, nodes );
            }
            nodes.add( nodeId );
        }
    }

    private class LabelScanViewIdIterator implements PrimitiveLongResourceIterator
    {
        private final LabelScanReader labelScanReader;
        private PrimitiveLongIterator idIterator;

        LabelScanViewIdIterator( LabelScanStore labelScanStore, int[] labelIds )
        {
            labelScanReader = labelScanStore.newReader();
            idIterator = labelScanReader.nodesWithAnyOfLabels( labelIds );
        }

        @Override
        public void close()
        {
            labelScanReader.close();
        }

        @Override
        public boolean hasNext()
        {
            return idIterator.hasNext();
        }

        @Override
        public long next()
        {
            return idIterator.next();
        }
    }
}
