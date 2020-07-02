/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.common.Subject;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;

import static org.neo4j.collection.PrimitiveLongCollections.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Factory for applier that gather node and property changes,
 * converting them into logical updates to the indexes.
 */
public class IndexTransactionApplierFactory implements TransactionApplierFactory
{
    private final IndexUpdateListener indexUpdateListener;

    public IndexTransactionApplierFactory( IndexUpdateListener indexUpdateListener )
    {
        this.indexUpdateListener = indexUpdateListener;
    }

    @Override
    public TransactionApplier startTx( CommandsToApply commands, BatchContext batchContext )
    {
        return new SingleTransactionApplier( commands, batchContext );
    }

    /**
     * Made as an internal non-static class here since the batch applier has so much interaction with
     * the transaction applier such that keeping them apart would incur too much data structures and interfaces
     * purely for communicating between the two to make the code hard to read.
     */
    private class SingleTransactionApplier extends TransactionApplier.Adapter
    {
        private final long txId;
        private final Subject subject;
        private final PropertyCommandsExtractor indexUpdatesExtractor = new PropertyCommandsExtractor();
        private List<IndexDescriptor> createdIndexes;
        private final IndexActivator indexActivator;
        private final BatchContext batchContext;

        SingleTransactionApplier( CommandsToApply commands, BatchContext batchContext )
        {
            this.txId = commands.transactionId();
            this.subject = commands.subject();
            this.indexActivator = batchContext.getIndexActivator();
            this.batchContext = batchContext;
        }

        @Override
        public void close() throws IOException
        {
            if ( indexUpdatesExtractor.containsAnyEntityOrPropertyUpdate() )
            {
                // Queue the index updates. When index updates from all transactions in this batch have been accumulated
                // we'll feed them to the index updates work sync at the end of the batch
                batchContext.indexUpdates().feed( indexUpdatesExtractor.getNodeCommands(), indexUpdatesExtractor.getRelationshipCommands() );
                indexUpdatesExtractor.close();
            }

            // Created pending indexes
            if ( createdIndexes != null )
            {
                indexUpdateListener.createIndexes( subject, createdIndexes.toArray( new IndexDescriptor[0] ) );
                createdIndexes = null;
            }
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
                    batchContext.labelUpdates().add( EntityTokenUpdate.tokenChanges( command.getKey(), labelsBefore, labelsAfter, txId ) );
                }
            }

            // for indexes
            return indexUpdatesExtractor.visitNodeCommand( command );
        }

        @Override
        public boolean visitRelationshipCommand( Command.RelationshipCommand command )
        {
            // for relationship type scan store updates
            RelationshipRecord before = command.getBefore();
            RelationshipRecord after = command.getAfter();

            int beforeType = before.getType();
            int afterType = after.getType();
            long[] beforeArray = beforeType == -1 || !before.inUse() ? EMPTY_LONG_ARRAY : new long[]{beforeType};
            long[] afterArray = afterType == -1 || !after.inUse() ? EMPTY_LONG_ARRAY : new long[]{afterType};
            if ( !Arrays.equals( beforeArray, afterArray ) )
            {
                batchContext.relationshipTypeUpdates().add( EntityTokenUpdate.tokenChanges( command.getKey(), beforeArray, afterArray ) );
            }

            return indexUpdatesExtractor.visitRelationshipCommand( command );
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command )
        {
            return indexUpdatesExtractor.visitPropertyCommand( command );
        }

        @Override
        public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
        {
            SchemaRule schemaRule = command.getSchemaRule();
            processSchemaCommand( command.getMode(), schemaRule );
            return false;
        }

        private void processSchemaCommand( Command.Mode commandMode, SchemaRule schemaRule ) throws IOException
        {
            if ( schemaRule instanceof IndexDescriptor )
            {
                IndexDescriptor indexRule = (IndexDescriptor) schemaRule;
                // Why apply index updates here? Here's the thing... this is a batch applier, which means that
                // index updates are gathered throughout the batch and applied in the end of the batch.
                // Assume there are some transactions creating or modifying nodes that may not be covered
                // by an existing index, but a later transaction in the same batch creates such an index.
                // In that scenario the index would be created, populated and then fed the [this time duplicate]
                // update for the node created before the index. The most straight forward solution is to
                // apply pending index updates up to this point in this batch before index schema changes occur.
                batchContext.applyPendingLabelAndIndexUpdates();

                switch ( commandMode )
                {
                case UPDATE:
                    // Shouldn't we be more clear about that we are waiting for an index to come online here?
                    // right now we just assume that an update to index records means wait for it to be online.
                    if ( indexRule.isUnique() )
                    {
                        // Register activations into the IndexActivator instead of IndexingService to avoid deadlock
                        // that could ensue for applying batches of transactions where a previous transaction in the same
                        // batch acquires a low-level commit lock that prevents the very same index population to complete.
                        indexActivator.activateIndex( indexRule );
                    }
                    break;
                case CREATE:
                    // Add to list so that all these indexes will be created in one call later
                    createdIndexes = createdIndexes == null ? new ArrayList<>() : createdIndexes;
                    createdIndexes.add( indexRule );
                    break;
                case DELETE:
                    indexUpdateListener.dropIndex( indexRule );
                    indexActivator.indexDropped( indexRule );
                    break;
                default:
                    throw new IllegalStateException( commandMode.name() );
                }
            }
        }
    }
}
