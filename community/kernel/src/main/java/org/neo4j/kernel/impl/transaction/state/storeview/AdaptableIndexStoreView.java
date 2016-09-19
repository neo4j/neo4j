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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
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
import org.neo4j.kernel.impl.api.index.StoreScan;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.register.Registers;
import org.neo4j.unsafe.impl.internal.dragons.FeatureToggles;

/**
 * Store view that will try to use label scan store {@link LabelScanStore} for cases when estimated number of nodes
 * is bellow certain threshold otherwise will fallback to whole store scan
 */
public class AdaptableIndexStoreView extends NeoStoreIndexStoreView
{
    private static final int VISIT_ALL_NODES_THRESHOLD_PERCENTAGE =
            FeatureToggles.getInteger( AdaptableIndexStoreView.class, "all.nodes.visit.percentage.threshold", 50 );

    private final LabelScanStore labelScanStore;
    private final CountsTracker counts;
    //TODO: we need to remember what kind of scan we do currently to handle updates in proper way
    private boolean usingLabelScan = true;

    PrimitiveLongObjectMap<PrimitiveLongObjectMap<PrimitiveLongSet>> propertyLabelNodes = Primitive.longObjectMap();


    public AdaptableIndexStoreView( LabelScanStore labelScanStore, LockService locks, NeoStores neoStores )
    {
        super( locks, neoStores );
        this.counts = neoStores.getCounts();
        this.labelScanStore = labelScanStore;
    }

    @Override
    public <FAILURE extends Exception> StoreScan<FAILURE> visitNodes( int[] labelIds,
            IntPredicate propertyKeyIdFilter, Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor,
            Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor )
    {
        if ( ArrayUtils.isEmpty( labelIds ) || isEmptyLabelScanStore() || isNumberOfLabeledNodesExceedThreshold( labelIds ) )
        {
            usingLabelScan = false;
            return super.visitNodes( labelIds, propertyKeyIdFilter, propertyUpdatesVisitor, labelUpdateVisitor );
        }
        usingLabelScan = true;
        return new LabelScanViewNodeStoreScan<>( nodeStore, locks, propertyStore, labelScanStore, labelUpdateVisitor,
                propertyUpdatesVisitor, labelIds, propertyKeyIdFilter );
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
        if ( usingLabelScan )
        {
            long[] labels;
            switch ( update.getUpdateMode() )
            {
                case ADDED:
                    labels = update.getLabelsAfter();
                    break;
                case CHANGED:
                    //TODO: should it be like that????
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
                // TODO: put if absent ?
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
        else
        {
            super.acceptUpdate( updater, update, currentlyIndexedNodeId );
        }
    }

    @Override
    public Property getProperty( long nodeId, int propertyKeyId ) throws EntityNotFoundException
    {
        return super.getProperty( nodeId, propertyKeyId );
    }

    @Override
    public boolean isFullScan()
    {
        return !usingLabelScan;
    }

    //    @Override
    public void complete( IndexPopulator indexPopulator, IndexDescriptor descriptor )
            throws EntityNotFoundException, PropertyNotFoundException, IOException, IndexEntryConflictException
    {
        if (usingLabelScan)
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
                    try ( IndexUpdater updater = indexPopulator.newPopulatingUpdater( this ) )
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
                                Property property = getProperty( nodeId, propertyKeyId );
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
    }

    private boolean isEmptyLabelScanStore()
    {
        return labelScanStore.allNodeLabelRanges().maxCount() == 0;
    }

    private boolean isNumberOfLabeledNodesExceedThreshold( int[] labelIds )
    {
        return getNumberOfLabeledNodes( labelIds ) > getVisitAllNodesThreshold();
    }

    private long getVisitAllNodesThreshold()
    {
        return (long) ((VISIT_ALL_NODES_THRESHOLD_PERCENTAGE / 100f) * nodeStore.getHighestPossibleIdInUse());
    }

    private long getNumberOfLabeledNodes( int[] labelIds )
    {
        return Arrays.stream( labelIds )
                .mapToLong( labelId -> counts.nodeCount( labelId, Registers.newDoubleLongRegister() ).readSecond() )
                .reduce( Math::addExact )
                .orElse( 0L );
    }
}
