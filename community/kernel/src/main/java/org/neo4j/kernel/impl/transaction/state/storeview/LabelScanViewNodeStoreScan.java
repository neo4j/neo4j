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

import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreIdIterator;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Store scan view that will try to minimize amount of scanned nodes by using label scan store {@link LabelScanStore}
 * as a source of known labeled node ids.
 * Label scan store reader will be aware only about nodes that where in the index at the moment of reader creation
 * soon as we finish iteration over ids from label scan store we will continue iterate over all nodes that where
 * added into the store after that.
 * @param <FAILURE>
 */
public class LabelScanViewNodeStoreScan<FAILURE extends Exception> extends StoreViewNodeStoreScan<FAILURE>
{
    private final LabelScanStore labelScanStore;

    public LabelScanViewNodeStoreScan( NodeStore nodeStore, LockService locks, PropertyStore propertyStore,
            LabelScanStore labelScanStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
            IntPredicate propertyKeyIdFilter )
    {
        super( nodeStore, locks, propertyStore, labelUpdateVisitor, propertyUpdatesVisitor, labelIds,
                propertyKeyIdFilter );
        this.labelScanStore = labelScanStore;
    }

    @Override
    protected PrimitiveLongResourceIterator getNodeIdIterator()
    {
        return new LabelScanViewIdIterator( labelScanStore, nodeStore, labelIds );
    }

    private class LabelScanViewIdIterator implements PrimitiveLongResourceIterator
    {
        private final NodeStore nodeStore;
        private final LabelScanReader labelScanReader;
        private long highestPossibleIdInUse;
        private boolean iterateOverNewNodeIds = false;
        private PrimitiveLongIterator idIterator;

        LabelScanViewIdIterator( LabelScanStore labelScanStore, NodeStore nodeStore, int[] labelIds )
        {
            this.nodeStore = nodeStore;
            labelScanReader = labelScanStore.newReader();
            highestPossibleIdInUse = labelScanReader.getMinIndexedNodeId();
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
            boolean hasNext = idIterator.hasNext();
            if ( !hasNext )
            {
                if ( iterateOverNewNodeIds )
                {
                    return false;
                }
                else
                {
                    idIterator = new StoreIdIterator( nodeStore, true, highestPossibleIdInUse + 1 );
                    iterateOverNewNodeIds = true;
                    return idIterator.hasNext();
                }
            }
            return true;
        }

        @Override
        public long next()
        {
            long value = idIterator.next();
            highestPossibleIdInUse = Math.max( highestPossibleIdInUse, value );
            return value;
        }
    }
}
