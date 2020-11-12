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
package org.neo4j.internal.batchimport;

import java.util.function.Supplier;

import org.neo4j.internal.batchimport.cache.ByteArray;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache;
import org.neo4j.internal.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;
import org.neo4j.internal.batchimport.cache.NodeType;
import org.neo4j.internal.batchimport.staging.ProducerStep;
import org.neo4j.internal.batchimport.staging.RecordDataAssembler;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;

import static java.lang.System.nanoTime;

/**
 * Using the {@link NodeRelationshipCache} efficiently looks for changed nodes and reads those
 * {@link NodeRecord} and sends downwards.
 */
public class ReadGroupRecordsByCacheStep extends ProducerStep
{
    private static final String READ_RELATIONSHIP_GROUPS_STEP_TAG = "readRelationshipGroupsStep";
    private final RecordStore<RelationshipGroupRecord> store;
    private final NodeRelationshipCache cache;
    private final PageCacheTracer pageCacheTracer;

    public ReadGroupRecordsByCacheStep( StageControl control, Configuration config,
            RecordStore<RelationshipGroupRecord> store, NodeRelationshipCache cache, PageCacheTracer pageCacheTracer )
    {
        super( control, config );
        this.store = store;
        this.cache = cache;
        this.pageCacheTracer = pageCacheTracer;
    }

    @Override
    protected void process()
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( READ_RELATIONSHIP_GROUPS_STEP_TAG );
              NodeVisitor visitor = new NodeVisitor( cursorTracer ) )
        {
            cache.visitChangedNodes( visitor, NodeType.NODE_TYPE_DENSE );
        }
    }

    private class NodeVisitor implements NodeChangeVisitor, AutoCloseable, NodeRelationshipCache.GroupVisitor, Supplier<RelationshipGroupRecord[]>
    {
        private final RecordDataAssembler<RelationshipGroupRecord> assembler = new RecordDataAssembler<>( store::newRecord,
                false /*In this scenario we know exactly which node IDs we're visiting, so we can be a bit more strict*/ );
        private final PageCursorTracer cursorTracer;
        private RelationshipGroupRecord[] batch = get();
        private int cursor;
        private long time = nanoTime();

        NodeVisitor( PageCursorTracer cursorTracer )
        {
            this.cursorTracer = cursorTracer;
        }

        @Override
        public void change( long nodeId, ByteArray array )
        {
            cache.getFirstRel( nodeId, this );
        }

        @Override
        public long visit( long nodeId, int typeId, long out, long in, long loop )
        {
            long id = store.nextId( cursorTracer );
            RelationshipGroupRecord record = batch[cursor++];
            record.setId( id );
            record.initialize( true, typeId, out, in, loop, nodeId, loop );
            if ( cursor == batchSize )
            {
                send();
                batch = control.reuse( this );
                cursor = 0;
            }
            return id;
        }

        private void send()
        {
            totalProcessingTime.add( nanoTime() - time );
            sendDownstream( batch );
            time = nanoTime();
            assertHealthy();
        }

        @Override
        public void close()
        {
            if ( cursor > 0 )
            {
                batch = assembler.cutOffAt( batch, cursor );
                send();
            }
        }

        @Override
        public RelationshipGroupRecord[] get()
        {
            return assembler.newBatchObject( batchSize );
        }
    }

    @Override
    protected long position()
    {
        return store.getHighId() * store.getRecordSize();
    }
}
