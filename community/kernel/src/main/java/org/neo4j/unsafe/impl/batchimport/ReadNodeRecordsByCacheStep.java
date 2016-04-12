/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;
import org.neo4j.unsafe.impl.batchimport.staging.AbstractStep;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.System.nanoTime;

/**
 * Using the {@link NodeRelationshipCache} efficiently looks for changed nodes and reads those
 * {@link NodeRecord} and sends downwards.
 */
public class ReadNodeRecordsByCacheStep extends AbstractStep<NodeRecord[]>
{
    private final boolean denseNodes;
    private final NodeRelationshipCache cache;
    private final int batchSize;
    private final NodeStore nodeStore;

    public ReadNodeRecordsByCacheStep( StageControl control, Configuration config,
            NodeStore nodeStore, NodeRelationshipCache cache, boolean denseNodes )
    {
        super( control, ">", config );
        this.nodeStore = nodeStore;
        this.cache = cache;
        this.denseNodes = denseNodes;
        this.batchSize = config.batchSize();
    }

    @Override
    public long receive( long ticket, NodeRecord[] batch )
    {
        new Thread()
        {
            @Override
            public void run()
            {
                assertHealthy();
                try ( NodeVisitor visitor = new NodeVisitor() )
                {
                    cache.visitChangedNodes( visitor, denseNodes );
                }
                endOfUpstream();
            }
        }.start();
        return 0;
    }

    private class NodeVisitor implements NodeChangeVisitor, AutoCloseable
    {
        private NodeRecord[] batch = new NodeRecord[batchSize];
        private int cursor;
        private long time = nanoTime();

        @Override
        public void change( long nodeId, ByteArray array )
        {
            batch[cursor++] = nodeStore.getRecord( nodeId, nodeStore.newRecord(), RecordLoad.CHECK );
            if ( cursor == batchSize )
            {
                send();
                batch = new NodeRecord[batchSize];
                cursor = 0;
            }
        }

        @SuppressWarnings( "unchecked" )
        private void send()
        {
            totalProcessingTime.add( nanoTime() - time );
            downstream.receive( doneBatches.getAndIncrement(), batch );
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
}
