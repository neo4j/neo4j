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
package org.neo4j.kernel.impl.transaction.state;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.api.index.NodeUpdates;
import org.neo4j.kernel.impl.api.index.PropertyPhysicalToLogicalConverter;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;

import static org.neo4j.kernel.impl.store.NodeLabelsField.parseLabelsField;

/**
 * Derives logical index updates from physical records, provided by {@link NodeCommand node commands} and
 * {@link PropertyCommand property commands}. For some types of updates state from store is also needed,
 * for example if adding a label to a node which already has properties matching existing and online indexes;
 * in that case the properties for that node needs to be read from store since the commands in that transaction
 * cannot itself provide enough information.
 *
 * One instance can be {@link #feed(PrimitiveLongObjectMap, PrimitiveLongObjectMap) fed} data about
 * multiple transactions, to be {@link #iterator() accessed} later.
 */
public class OnlineIndexUpdates implements IndexUpdates
{
    private final NodeStore nodeStore;
    private final IndexingUpdateService updateService;
    private final PropertyPhysicalToLogicalConverter converter;
    private final Collection<IndexEntryUpdate<SchemaDescriptor>> updates = new ArrayList<>();
    private NodeRecord nodeRecord;

    public OnlineIndexUpdates( NodeStore nodeStore,
                             IndexingUpdateService updateService,
                             PropertyPhysicalToLogicalConverter converter )
    {
        this.nodeStore = nodeStore;
        this.updateService = updateService;
        this.converter = converter;
    }

    @Override
    public Iterator<IndexEntryUpdate<SchemaDescriptor>> iterator()
    {
        return updates.iterator();
    }

    @Override
    public void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propertyCommands,
            PrimitiveLongObjectMap<NodeCommand> nodeCommands )
    {
        PrimitiveLongIterator nodeIds = allKeys( nodeCommands, propertyCommands ).iterator();
        while ( nodeIds.hasNext() )
        {
            long nodeId = nodeIds.next();
            gatherUpdatesFor( nodeId, nodeCommands.get( nodeId ), propertyCommands.get( nodeId ) );
        }
    }

    private PrimitiveLongSet allKeys( PrimitiveLongObjectMap... maps )
    {
        PrimitiveLongSet union = Primitive.longSet();
        for ( PrimitiveLongObjectMap map : maps )
        {
            union.addAll( map.iterator() );
        }
        return union;
    }

    @Override
    public boolean hasUpdates()
    {
        return !updates.isEmpty();
    }

    private void gatherUpdatesFor( long nodeId, NodeCommand nodeCommand, List<PropertyCommand> propertyCommands )
    {
        NodeUpdates.Builder nodePropertyUpdate =
                gatherUpdatesFromCommandsForNode( nodeId, nodeCommand, propertyCommands );

        NodeUpdates nodeUpdates = nodePropertyUpdate.build();
        // we need to materialize the IndexEntryUpdates here, because when we
        // consume (later in separate thread) the store might have changed.
        for ( IndexEntryUpdate<SchemaDescriptor> update :  updateService.convertToIndexUpdates( nodeUpdates ) )
        {
            updates.add( update );
        }
    }

    private NodeUpdates.Builder gatherUpdatesFromCommandsForNode( long nodeId,
            NodeCommand nodeChanges,
            List<PropertyCommand> propertyCommandsForNode )
    {
        long[] nodeLabelsBefore;
        long[] nodeLabelsAfter;
        if ( nodeChanges != null )
        {
            nodeLabelsBefore = parseLabelsField( nodeChanges.getBefore() ).get( nodeStore );
            nodeLabelsAfter = parseLabelsField( nodeChanges.getAfter() ).get( nodeStore );
        }
        else
        {
            /* If the node doesn't exist here then we've most likely encountered this scenario:
             * - TX1: Node N exists and has property record P
             * - rotate log
             * - TX2: P gets changed
             * - TX3: N gets deleted (also P, but that's irrelevant for this scenario)
             * - N is persisted to disk for some reason
             * - crash
             * - recover
             * - TX2: P has changed and updates to indexes are gathered. As part of that it tries to read
             *        the labels of N (which does not exist a.t.m.).
             *
             * We can actually (if we disregard any potential inconsistencies) just assume that
             * if this happens and we're in recovery mode that the node in question will be deleted
             * in an upcoming transaction, so just skip this update.
             */
            NodeRecord nodeRecord = loadNode( nodeId );
            nodeLabelsBefore = nodeLabelsAfter = parseLabelsField( nodeRecord ).get( nodeStore );
        }

        // First get possible Label changes
        NodeUpdates.Builder nodePropertyUpdates =
                NodeUpdates.forNode( nodeId, nodeLabelsBefore, nodeLabelsAfter );

        // Then look for property changes
        if ( propertyCommandsForNode != null )
        {
            converter.convertPropertyRecord( nodeId, Iterables.cast( propertyCommandsForNode ), nodePropertyUpdates );
        }
        return nodePropertyUpdates;
    }

    private NodeRecord loadNode( long nodeId )
    {
        if ( nodeRecord == null )
        {
            nodeRecord = nodeStore.newRecord();
        }
        nodeStore.getRecord( nodeId, nodeRecord, RecordLoad.NORMAL );
        return nodeRecord;
    }
}
