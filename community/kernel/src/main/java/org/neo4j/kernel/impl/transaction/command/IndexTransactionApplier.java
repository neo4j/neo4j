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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.concurrent.Work;
import org.neo4j.concurrent.WorkSync;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.SORT_BY_NODE_ID;
import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Gather node and property changes, converting them into logical updates to the indexes.
 * {@link #close()} will actually apply to the indexes.
 */
public class IndexTransactionApplier extends CommandHandler.Adapter
{
    private final ValidatedIndexUpdates indexUpdates;
    private List<NodeLabelUpdate> labelUpdates;

    private final IndexingService indexingService;
    private final WorkSync<Provider<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;

    public IndexTransactionApplier( IndexingService indexingService, ValidatedIndexUpdates indexUpdates,
                                    WorkSync<Provider<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync )
    {
        this.indexingService = indexingService;
        this.indexUpdates = indexUpdates;
        this.labelScanStoreSync = labelScanStoreSync;
    }

    @Override
    public void apply()
    {
        try
        {
            if ( labelUpdates != null )
            {
                updateLabelScanStore();
            }

            if ( indexUpdates.hasChanges() )
            {
                updateIndexes();
            }
        }
        catch ( IOException | IndexCapacityExceededException | IndexEntryConflictException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private void updateIndexes() throws IOException, IndexCapacityExceededException, IndexEntryConflictException
    {
        // We only allow a single writer at the time to update the schema index stores
        // TODO we should probably do two things here:
        //   - make the synchronized block more granular, over each index or something
        //   - not even on each index, but make the decision up to the index implementation instead
        synchronized ( indexingService )
        {
            indexUpdates.flush();
        }
    }

    private void updateLabelScanStore()
    {
        // Updates are sorted according to node id here, an artifact of node commands being sorted
        // by node id when extracting from TransactionRecordState.
        labelScanStoreSync.apply(
                new LabelUpdateWork( labelUpdates ) );
    }

    public static class LabelUpdateWork implements Work<Provider<LabelScanWriter>,LabelUpdateWork>
    {
        private final List<NodeLabelUpdate> labelUpdates;

        public LabelUpdateWork( List<NodeLabelUpdate> labelUpdates )
        {
            this.labelUpdates = labelUpdates;
        }

        @Override
        public LabelUpdateWork combine( LabelUpdateWork work )
        {
            labelUpdates.addAll( work.labelUpdates );
            return this;
        }

        @Override
        public void apply( Provider<LabelScanWriter> labelScanStore )
        {
            Collections.sort( labelUpdates, SORT_BY_NODE_ID );
            try ( LabelScanWriter writer = labelScanStore.instance() )
            {
                for ( NodeLabelUpdate update : labelUpdates )
                {
                    writer.write( update );
                }
            }
            catch ( Exception e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        // for label store updates
        NodeRecord before = command.getBefore();
        NodeRecord after = command.getAfter();

        NodeLabels labelFieldBefore = parseLabelsField( before );
        NodeLabels labelFieldAfter = parseLabelsField( after );
        if ( !(labelFieldBefore.isInlined() && labelFieldAfter.isInlined()
               && before.getLabelField() == after.getLabelField()) )
        {
            long[] labelsBefore = labelFieldBefore.getIfLoaded();
            long[] labelsAfter = labelFieldAfter.getIfLoaded();
            if ( labelsBefore != null && labelsAfter != null )
            {
                addLabelUpdate( NodeLabelUpdate.labelChanges( command.getKey(), labelsBefore, labelsAfter ) );
            }
        }

        return false;
    }

    private void addLabelUpdate( NodeLabelUpdate labelChanges )
    {
        if ( labelUpdates == null )
        {
            labelUpdates = new ArrayList<>();
        }
        labelUpdates.add( labelChanges );
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command ) throws IOException
    {
        if ( command.getSchemaRule() instanceof IndexRule )
        {
            switch ( command.getMode() )
            {
            case UPDATE:
                // Shouldn't we be more clear about that we are waiting for an index to come online here?
                // right now we just assume that an update to index records means wait for it to be online.
                if ( ((IndexRule) command.getSchemaRule()).isConstraintIndex() )
                {
                    try
                    {
                        indexingService.activateIndex( command.getSchemaRule().getId() );
                    }
                    catch ( IndexNotFoundKernelException | IndexActivationFailedKernelException |
                            IndexPopulationFailedKernelException e )
                    {
                        throw new IllegalStateException(
                                "Unable to enable constraint, backing index is not online.", e );
                    }
                }
                break;
            case CREATE:
                indexingService.createIndex( (IndexRule) command.getSchemaRule() );
                break;
            case DELETE:
                indexingService.dropIndex( (IndexRule) command.getSchemaRule() );
                break;
            default:
                throw new IllegalStateException( command.getMode().name() );
            }
        }
        return false;
    }
}
