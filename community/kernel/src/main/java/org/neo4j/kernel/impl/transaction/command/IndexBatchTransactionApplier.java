/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

/**
 * Gather node and property changes, converting them into logical updates to the indexes. {@link #close()} will actually
 * apply the indexes.
 */
public class IndexBatchTransactionApplier implements BatchTransactionApplier
{
    private final IndexingService indexingService;
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final PropertyLoader propertyLoader;
    private Set<IndexDescriptor> affectedIndexes;
    private List<NodeLabelUpdate> labelUpdates;

    public IndexBatchTransactionApplier( IndexingService indexingService,
            WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync,
            NodeStore nodeStore, PropertyStore propertyStore, PropertyLoader propertyLoader )
    {
        this.indexingService = indexingService;
        this.labelScanStoreSync = labelScanStoreSync;
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.propertyLoader = propertyLoader;

    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction )
    {
        return new IndexTransactionApplier( indexingService, nodeLabelUpdate -> {
            if ( labelUpdates == null )
            {
                labelUpdates = new ArrayList<>();
            }
            labelUpdates.add( nodeLabelUpdate );
        }, affectedIndexes, nodeStore, propertyStore, propertyLoader );
    }

    @Override
    public TransactionApplier startTx( TransactionToApply transaction, LockGroup lockGroup ) throws IOException
    {
        return startTx( transaction );
    }

    @Override
    public void close() throws Exception
    {
        // Apply all the label updates within this whole batch of transactions.
        if ( labelUpdates != null )
        {
            // Updates are sorted according to node id here, an artifact of node commands being sorted
            // by node id when extracting from TransactionRecordState.
            labelScanStoreSync.apply( new LabelUpdateWork( labelUpdates ) );
        }

        if ( affectedIndexes != null )
        {
            // Since we have written changes to indexes w/o refreshing readers, then do so now
            // at this point where all changes in this whole batch of transactions have been applied.
            indexingService.flushAll( affectedIndexes );
        }
    }
}
