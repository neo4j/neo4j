/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.store.NodeLabels;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.state.LazyIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;
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

    private final Map<Long,NodeCommand> nodeCommands = new HashMap<>();
    private final Map<Long,List<PropertyCommand>> propertyCommands = new HashMap<>();
    private final List<NodeLabelUpdate> labelUpdates = new ArrayList<>();

    private final IndexingService indexingService;
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final CacheAccessBackDoor cacheAccess;
    private final PropertyLoader propertyLoader;
    private final long transactionId;
    private final TransactionApplicationMode mode;
    private final WorkSync<LabelScanStore,LabelUpdateWork> labelScanStoreSync;

    public IndexTransactionApplier( IndexingService indexingService,
                                    NodeStore nodeStore, PropertyStore propertyStore, CacheAccessBackDoor cacheAccess,
                                    PropertyLoader propertyLoader, long transactionId, TransactionApplicationMode mode,
                                    WorkSync<LabelScanStore,LabelUpdateWork> labelScanStoreSync )
    {
        this.indexingService = indexingService;
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.cacheAccess = cacheAccess;
        this.propertyLoader = propertyLoader;
        this.transactionId = transactionId;
        this.mode = mode;
        this.labelScanStoreSync = labelScanStoreSync;
    }

    @Override
    public void apply()
    {
        if ( !labelUpdates.isEmpty() )
        {
            updateLabelScanStore();
            cacheAccess.applyLabelUpdates( labelUpdates );
        }

        if ( !nodeCommands.isEmpty() || !propertyCommands.isEmpty() )
        {
            updateIndexes();
        }
    }

    private void updateIndexes()
    {
        LazyIndexUpdates updates = new LazyIndexUpdates(
                nodeStore, propertyStore, propertyCommands, nodeCommands, propertyLoader );

        // We only allow a single writer at the time to update the schema index stores
        synchronized ( indexingService )
        {
            indexingService.updateIndexes( updates, transactionId, mode.needsIdempotencyChecks() );
        }
    }

    private void updateLabelScanStore()
    {
        labelScanStoreSync.apply(
                new LabelUpdateWork( new ArrayList<>( labelUpdates ) ) );

//        // We only allow a single writer at the time to update the label scan store
//        synchronized ( labelScanStore )
//        {
//            try ( LabelScanWriter writer = labelScanStore.newWriter() )
//            {
//                for ( NodeLabelUpdate update : labelUpdates )
//                {
//                    writer.write( update );
//                }
//            }
//            catch ( IOException e )
//            {
//                throw new UnderlyingStorageException( e );
//            }
//        }
    }

    public static class LabelUpdateWork implements WorkSync.CombinableWork<LabelScanStore,LabelUpdateWork>
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
        public void apply( LabelScanStore labelScanStore )
        {
            Collections.sort( labelUpdates, nodeLabelUpdateComparator );
            try ( LabelScanWriter writer = labelScanStore.newWriter() )
            {
                for ( NodeLabelUpdate update : labelUpdates )
                {
                    writer.write( update );
                }
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command ) throws IOException
    {
        // for index updates
        nodeCommands.put( command.getKey(), command );

        NodeRecord before = command.getBefore();
        NodeRecord after = command.getAfter();

        // for label store updates
        NodeLabels labelFieldBefore = parseLabelsField( before );
        NodeLabels labelFieldAfter = parseLabelsField( after );
        if ( !(labelFieldBefore.isInlined() && labelFieldAfter.isInlined()
               && before.getLabelField() == after.getLabelField()) )
        {
            long[] labelsBefore = labelFieldBefore.getIfLoaded();
            long[] labelsAfter = labelFieldAfter.getIfLoaded();
            if ( labelsBefore != null && labelsAfter != null )
            {
                labelUpdates.add( NodeLabelUpdate.labelChanges( command.getKey(), labelsBefore, labelsAfter ) );
            }
        }

        return false;
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
    {
        PropertyRecord record = command.getAfter();
        if ( record.isNodeSet() )
        {
            long nodeId = command.getAfter().getNodeId();
            List<PropertyCommand> group = propertyCommands.get( nodeId );
            if ( group == null )
            {
                propertyCommands.put( nodeId, group = new ArrayList<>() );
            }
            group.add( command );
        }
        return false;
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
