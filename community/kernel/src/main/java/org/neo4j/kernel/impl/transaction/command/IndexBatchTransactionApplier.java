/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import org.neo4j.concurrent.AsyncApply;
import org.neo4j.concurrent.WorkSync;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.BatchTransactionApplier;
import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.api.index.NodePropertyCommandsExtractor;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;
import org.neo4j.kernel.impl.transaction.state.OnlineIndexUpdates;
import org.neo4j.storageengine.api.CommandsToApply;

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Gather node and property changes, converting them into logical updates to the indexes. {@link #close()} will actually
 * apply the indexes.
 */
public class IndexBatchTransactionApplier extends BatchTransactionApplier.Adapter
{
    private final IndexingService indexingService;
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;
    private final WorkSync<IndexingUpdateService,IndexUpdatesWork> indexUpdatesSync;
    private final SingleTransactionApplier transactionApplier;
    private final IndexActivator indexActivator;
    private final PropertyPhysicalToLogicalConverter indexUpdateConverter;

    private List<NodeLabelUpdate> labelUpdates;
    private IndexUpdates indexUpdates;
    private long txId;

    public IndexBatchTransactionApplier( IndexingService indexingService, WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync,
            WorkSync<IndexingUpdateService,IndexUpdatesWork> indexUpdatesSync, NodeStore nodeStore, PropertyPhysicalToLogicalConverter indexUpdateConverter,
            IndexActivator indexActivator )
    {
        this.indexingService = indexingService;
        this.labelScanStoreSync = labelScanStoreSync;
        this.indexUpdatesSync = indexUpdatesSync;
        this.indexUpdateConverter = indexUpdateConverter;
        this.transactionApplier = new SingleTransactionApplier( nodeStore );
        this.indexActivator = indexActivator;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply transaction )
    {
        txId = transaction.transactionId();
        return transactionApplier;
    }

    private void applyPendingLabelAndIndexUpdates() throws IOException
    {
        AsyncApply labelUpdatesApply = null;
        if ( labelUpdates != null )
        {
            // Updates are sorted according to node id here, an artifact of node commands being sorted
            // by node id when extracting from TransactionRecordState.
            labelUpdatesApply = labelScanStoreSync.applyAsync( new LabelUpdateWork( labelUpdates ) );
            labelUpdates = null;
        }
        if ( indexUpdates != null && indexUpdates.hasUpdates() )
        {
            try
            {
                indexUpdatesSync.apply( new IndexUpdatesWork( indexUpdates ) );
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Failed to flush index updates", e );
            }
            indexUpdates = null;
        }

        if ( labelUpdatesApply != null )
        {
            try
            {
                labelUpdatesApply.await();
            }
            catch ( ExecutionException e )
            {
                throw new IOException( "Failed to flush label updates", e );
            }
        }
    }

    @Override
    public void close() throws Exception
    {
        applyPendingLabelAndIndexUpdates();
    }

    /**
     * Made as an internal non-static class here since the batch applier has so much interaction with
     * the transaction applier such that keeping them apart would incur too much data structures and interfaces
     * purely for communicating between the two to make the code hard to read.
     */
    private class SingleTransactionApplier extends TransactionApplier.Adapter
    {
        private final NodeStore nodeStore;
        private final NodePropertyCommandsExtractor indexUpdatesExtractor = new NodePropertyCommandsExtractor();
        private List<IndexRule> createdIndexes;

        SingleTransactionApplier( NodeStore nodeStore )
        {
            this.nodeStore = nodeStore;
        }

        @Override
        public void close() throws Exception
        {
            if ( indexUpdatesExtractor.containsAnyNodeOrPropertyUpdate() )
            {
                // Queue the index updates. When index updates from all transactions in this batch have been accumulated
                // we'll feed them to the index updates work sync at the end of the batch
                indexUpdates().feed( indexUpdatesExtractor.propertyCommandsByNodeIds(),
                        indexUpdatesExtractor.nodeCommandsById() );
                indexUpdatesExtractor.close();
            }

            // Created pending indexes
            if ( createdIndexes != null )
            {
                indexingService.createIndexes( createdIndexes.toArray( new IndexRule[createdIndexes.size()] ) );
                createdIndexes = null;
            }
        }

        private IndexUpdates indexUpdates()
        {
            if ( indexUpdates == null )
            {
                indexUpdates = new OnlineIndexUpdates( nodeStore, indexingService, indexUpdateConverter );
            }
            return indexUpdates;
        }

        @Override
        public boolean visitNodeCommand( Command.NodeCommand command )
        {
            // for label store updates
            NodeRecord before = command.getBefore();
            NodeRecord after = command.getAfter();

            NodeLabels labelFieldBefore = parseLabelsField( before );
            NodeLabels labelFieldAfter = parseLabelsField( after );
            if ( !(labelFieldBefore.isInlined() && labelFieldAfter.isInlined() &&
                    before.getLabelField() == after.getLabelField()) )
            {
                long[] labelsBefore = labelFieldBefore.getIfLoaded();
                long[] labelsAfter = labelFieldAfter.getIfLoaded();
                if ( labelsBefore != null && labelsAfter != null )
                {
                    if ( labelUpdates == null )
                    {
                        labelUpdates = new ArrayList<>();
                    }
                    labelUpdates.add( NodeLabelUpdate.labelChanges( command.getKey(), labelsBefore, labelsAfter, txId ) );
                }
            }

            // for indexes
            return indexUpdatesExtractor.visitNodeCommand( command );
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command )
        {
            return indexUpdatesExtractor.visitPropertyCommand( command );
        }

        @Override
        public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
        {
            if ( command.getSchemaRule() instanceof IndexRule )
            {
                // Why apply index updates here? Here's the thing... this is a batch applier, which means that
                // index updates are gathered throughout the batch and applied in the end of the batch.
                // Assume there are some transactions creating or modifying nodes that may not be covered
                // by an existing index, but a later transaction in the same batch creates such an index.
                // In that scenario the index would be created, populated and then fed the [this time duplicate]
                // update for the node created before the index. The most straight forward solution is to
                // apply pending index updates up to this point in this batch before index schema changes occur.
                applyPendingLabelAndIndexUpdates();

                switch ( command.getMode() )
                {
                case UPDATE:
                    // Shouldn't we be more clear about that we are waiting for an index to come online here?
                    // right now we just assume that an update to index records means wait for it to be online.
                    if ( ((IndexRule) command.getSchemaRule()).canSupportUniqueConstraint() )
                    {
                        // Register activations into the IndexActivator instead of IndexingService to avoid deadlock
                        // that could insue for applying batches of transactions where a previous transaction in the same
                        // batch acquires a low-level commit lock that prevents the very same index population to complete.
                        indexActivator.activateIndex( command.getSchemaRule().getId() );
                    }
                    break;
                case CREATE:
                    // Add to list so that all these indexes will be created in one call later
                    createdIndexes = createdIndexes == null ? new ArrayList<>() : createdIndexes;
                    createdIndexes.add( (IndexRule) command.getSchemaRule() );
                    break;
                case DELETE:
                    indexingService.dropIndex( (IndexRule) command.getSchemaRule() );
                    indexActivator.indexDropped( command.getSchemaRule().getId() );
                    break;
                default:
                    throw new IllegalStateException( command.getMode().name() );
                }
            }
            return false;
        }
    }
}
