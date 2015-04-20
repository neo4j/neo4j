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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexCapacityExceededException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
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

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Gather node and property changes, converting them into logical updates to the indexes.
 * {@link #close()} will actually apply to the indexes.
 */
public class IndexTransactionApplier extends NeoCommandHandler.Adapter
{
    private static final Comparator<NodeLabelUpdate> nodeLabelUpdateComparator = new Comparator<NodeLabelUpdate>()
    {
        @Override
        public int compare( NodeLabelUpdate o1, NodeLabelUpdate o2 )
        {
            return Long.compare( o1.getNodeId(), o2.getNodeId() );
        }
    };

    private final ValidatedIndexUpdates indexUpdates;
    private List<NodeLabelUpdate> labelUpdates;

    private final IndexingService indexingService;
    private final LabelScanStore labelScanStore;

    public IndexTransactionApplier( IndexingService indexingService, ValidatedIndexUpdates indexUpdates,
            LabelScanStore labelScanStore )
    {
        this.indexingService = indexingService;
        this.indexUpdates = indexUpdates;
        this.labelScanStore = labelScanStore;
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
        synchronized ( indexingService )
        {
            indexUpdates.flush();
        }
    }

    private void updateLabelScanStore() throws IOException, IndexCapacityExceededException
    {
        Collections.sort( labelUpdates, nodeLabelUpdateComparator );

        // We only allow a single writer at the time to update the label scan store
        synchronized ( labelScanStore )
        {
            try ( LabelScanWriter writer = labelScanStore.newWriter() )
            {
                for ( NodeLabelUpdate update : labelUpdates )
                {
                    writer.write( update );
                }
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
