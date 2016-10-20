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

import java.util.List;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.MultipleIndexPopulator;
import org.neo4j.kernel.impl.api.index.NodePropertyUpdates;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;

/**
 * Store scan view that will try to minimize amount of scanned nodes by using label scan store {@link LabelScanStore}
 * as a source of known labeled node ids.
 * @param <FAILURE>
 */
public class LabelScanViewNodeStoreScan<FAILURE extends Exception> extends StoreViewNodeStoreScan<FAILURE>
{
    private final LabelScanStore labelScanStore;
    // flag that indicated presence of concurrent updates that are not visible to current label index scan
    private boolean outdated;

    public LabelScanViewNodeStoreScan( NodeStore nodeStore, LockService locks,
            PropertyStore propertyStore,
            LabelScanStore labelScanStore, Visitor<NodeLabelUpdate,FAILURE> labelUpdateVisitor,
            Visitor<NodePropertyUpdates,FAILURE> propertyUpdatesVisitor, int[] labelIds,
            IntPredicate propertyKeyIdFilter )
    {
        super( nodeStore, locks, propertyStore, labelUpdateVisitor, propertyUpdatesVisitor, labelIds,
                propertyKeyIdFilter );
        this.labelScanStore = labelScanStore;
    }

    @Override
    public PrimitiveLongResourceIterator getNodeIdIterator()
    {
        return new LabelScanViewIdIterator( this, labelScanStore, labelIds );
    }

    @Override
    public void configure( List<MultipleIndexPopulator.IndexPopulation> populations )
    {
        populations.forEach( population -> population.populator.configureSampling( false ) );
    }

    @Override
    public void acceptUpdate( MultipleIndexPopulator.MultipleIndexUpdater updater, NodePropertyUpdate update,
            long currentlyIndexedNodeId )
    {
        super.acceptUpdate( updater, update, currentlyIndexedNodeId );
        if ( update.getNodeId() > currentlyIndexedNodeId )
        {
            markOutdated();
        }
    }

    boolean isOutdated()
    {
        return outdated;
    }

    void clearOutdatedFlag()
    {
        outdated = false;
    }

    private void markOutdated()
    {
        outdated = true;
    }

}
