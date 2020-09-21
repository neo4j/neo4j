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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;

import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.memory.MemoryTracker;

import static org.neo4j.internal.recordstorage.RelationshipGroupGetter.RelationshipGroupMonitor.EMPTY;

class MappedNodeDataLookup implements RelationshipCreator.NodeDataLookup
{
    private final MutableLongObjectMap<NodeContext> contexts;
    private final RelationshipGroupGetter relGroupGetter;
    private final RecordAccessSet recordChanges;
    private final PageCursorTracer cursorTracer;
    private final MemoryTracker memoryTracker;

    MappedNodeDataLookup( MutableLongObjectMap<NodeContext> contexts, RelationshipGroupGetter relGroupGetter, RecordAccessSet recordChanges,
            PageCursorTracer cursorTracer, MemoryTracker memoryTracker )
    {
        this.contexts = contexts;
        this.relGroupGetter = relGroupGetter;
        this.recordChanges = recordChanges;
        this.cursorTracer = cursorTracer;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public RecordProxy<RelationshipRecord,Void> entryPoint( long nodeId, int type, int dir )
    {
        NodeContext byNode = contexts.get( nodeId );
        if ( byNode != null )
        {
            NodeContext.DenseContext context = byNode.denseContextIfExists( type );
            if ( context != null )
            {
                return context.entryPoint( dir );
            }
        }
        return null;
    }

    @Override
    public RecordProxy<RelationshipGroupRecord,Integer> group( long nodeId, int type, boolean create )
    {
        // (Temporarily?) we can create groups lazily here
        NodeContext nodeContext = contexts.getIfAbsentPutWithKey( nodeId,
                n -> new NodeContext( recordChanges.getNodeRecords().getOrLoad( n, null, cursorTracer ), memoryTracker ) );
        NodeContext.DenseContext context = nodeContext.denseContext( type );
        RecordProxy<RelationshipGroupRecord,Integer> group = context.group();
        if ( group == null )
        {
            RecordProxy<NodeRecord,Void> nodeChange = recordChanges.getNodeRecords().getOrLoad( nodeId, null, cursorTracer );
            group = create
                    ? relGroupGetter.getOrCreateRelationshipGroup( nodeChange, type, recordChanges.getRelGroupRecords() )
                    : relGroupGetter.getRelationshipGroup( nodeChange.forReadingLinkage(), type, recordChanges.getRelGroupRecords(), EMPTY ).group();
            context.setGroup( group );
        }
        return group;
    }

    /**
     * Reads group from recordChanges, but also caches the group in the internal context.
     */
    public RecordProxy<RelationshipGroupRecord,Integer> group( long groupId )
    {
        RecordProxy<RelationshipGroupRecord,Integer> groupProxy = recordChanges.getRelGroupRecords().getOrLoad( groupId, null, cursorTracer );
        RelationshipGroupRecord group = groupProxy.forReadingData();
        NodeContext nodeContext = contexts.getIfAbsentPutWithKey( group.getOwningNode(),
                n -> new NodeContext( recordChanges.getNodeRecords().getOrLoad( n, null, cursorTracer ), memoryTracker ) );
        NodeContext.DenseContext context = nodeContext.denseContext( group.getType() );
        context.setGroup( groupProxy );
        return groupProxy;
    }
}
