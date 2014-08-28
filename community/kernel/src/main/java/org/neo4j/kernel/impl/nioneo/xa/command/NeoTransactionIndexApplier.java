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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.nioneo.store.labels.NodeLabels;
import org.neo4j.kernel.impl.nioneo.xa.LazyIndexUpdates;
import org.neo4j.kernel.impl.nioneo.xa.PropertyLoader;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.PropertyCommand;
import org.neo4j.unsafe.batchinsert.LabelScanWriter;

import static org.neo4j.kernel.impl.nioneo.store.labels.NodeLabelsField.parseLabelsField;

/**
 * Gather node and property changes, converting them into logical updates to the indexes.
 * {@link #close()} will actually apply to the indexes.
 */
public class NeoTransactionIndexApplier extends NeoCommandHandler.Adapter
{
    private static final Comparator<NodeLabelUpdate> nodeLabelUpdateComparator = new Comparator<NodeLabelUpdate>()
    {
        @Override
        public int compare( NodeLabelUpdate o1, NodeLabelUpdate o2 )
        {
            return Long.compare( o1.getNodeId(), o2.getNodeId() );
        }
    };

    private final Map<Long, NodeCommand> nodeCommands = new HashMap<>();
    private final Map<Long, List<PropertyCommand>> propertyCommands = new HashMap<>();
    private final List<NodeLabelUpdate> labelUpdates = new ArrayList<>();

    private final IndexingService indexingService;
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final LabelScanStore labelScanStore;
    private final CacheAccessBackDoor cacheAccess;
    private final PropertyLoader propertyLoader;

    public NeoTransactionIndexApplier( IndexingService indexingService, LabelScanStore labelScanStore,
            NodeStore nodeStore, PropertyStore propertyStore, CacheAccessBackDoor cacheAccess,
            PropertyLoader propertyLoader )
    {
        this.indexingService = indexingService;
        this.labelScanStore = labelScanStore;
        this.nodeStore = nodeStore;
        this.propertyStore = propertyStore;
        this.cacheAccess = cacheAccess;
        this.propertyLoader = propertyLoader;
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
            indexingService.updateIndexes( updates );
        }
    }

    private void updateLabelScanStore()
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

        return true;
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
        return true;
    }
}
