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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.concurrent.WorkSync;
import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.ValidatedIndexUpdates;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Gather node and property changes, converting them into logical updates to the indexes.
 * {@link #close()} will actually apply to the indexes.
 */
public class IndexTransactionApplier extends CommandHandler.Adapter
{
    private final IndexingService indexingService;
    private final WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync;

    private List<NodeLabelUpdate> labelUpdates;
    private Set<IndexDescriptor> affectedIndexes;
    private ValidatedIndexUpdates indexUpdates;

    public IndexTransactionApplier( IndexingService indexingService,
                                    WorkSync<Supplier<LabelScanWriter>,LabelUpdateWork> labelScanStoreSync )
    {
        this.indexingService = indexingService;
        this.labelScanStoreSync = labelScanStoreSync;
    }

    @Override
    public void begin( TransactionToApply transaction ) throws IOException
    {
        indexUpdates = transaction.validatedIndexUpdates();
    }

    @Override
    public void end() throws Exception
    {
        if ( indexUpdates.hasChanges() )
        {
            // Lazily initialize set to hold which indexes has changed.
            if ( affectedIndexes == null )
            {
                affectedIndexes = new HashSet<>();
            }

            // Write index updates. The setup should be that these updates doesn't do unnecessary
            // work that could be done after the whole batch, like f.ex. refreshing index readers.
            indexUpdates.flush( affectedIndexes );
            indexUpdates.close();
        }
    }

    @Override
    public void apply()
    {
        // Apply all the label updates within this whole batch of transactions.
        if ( labelUpdates != null )
        {
            updateLabelScanStore();
        }

        if ( affectedIndexes != null )
        {
            // Since we have written changes to indexes w/o refreshing readers, then do so now
            // at this point where all changes in this whole batch of transactions have been applied.
            indexingService.flushAll( affectedIndexes );
        }
    }

    private void updateLabelScanStore()
    {
        // Updates are sorted according to node id here, an artifact of node commands being sorted
        // by node id when extracting from TransactionRecordState.
        labelScanStoreSync.apply( new LabelUpdateWork( labelUpdates ) );
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
