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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.GroupVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.staging.ProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.System.nanoTime;

/**
 * Using the {@link NodeRelationshipCache} efficiently looks for changed nodes and reads those
 * {@link NodeRecord} and sends downwards.
 */
public class ReadGroupRecordsByCacheStep extends ProducerStep
{
    private final RecordStore<RelationshipGroupRecord> store;
    private final NodeRelationshipCache cache;

    public ReadGroupRecordsByCacheStep( StageControl control, Configuration config,
            RecordStore<RelationshipGroupRecord> store, NodeRelationshipCache cache )
    {
        super( control, config );
        this.store = store;
        this.cache = cache;
    }

    @Override
    protected void process()
    {
        try ( NodeVisitor visitor = new NodeVisitor() )
        {
            cache.visitChangedNodes( visitor, NodeType.NODE_TYPE_DENSE );
        }
    }

    private class NodeVisitor implements NodeChangeVisitor, AutoCloseable, GroupVisitor
    {
        private RelationshipGroupRecord[] batch = new RelationshipGroupRecord[batchSize];
        private int cursor;
        private long time = nanoTime();

        @Override
        public void change( long nodeId, ByteArray array )
        {
            cache.getFirstRel( nodeId, this );
        }

        @Override
        public long visit( long nodeId, int typeId, long out, long in, long loop )
        {
            long id = store.nextId();
            RelationshipGroupRecord record = store.newRecord();
            record.setId( id );
            batch[cursor++] = record.initialize( true, typeId, out, in, loop, nodeId, loop );
            if ( cursor == batchSize )
            {
                send();
                batch = new RelationshipGroupRecord[batchSize];
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
                send();
            }
        }
    }

    @Override
    protected long position()
    {
        return store.getHighId() * store.getRecordSize();
    }
}
