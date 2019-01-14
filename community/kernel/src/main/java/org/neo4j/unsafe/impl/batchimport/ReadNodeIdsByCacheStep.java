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
package org.neo4j.unsafe.impl.batchimport;

import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache.NodeChangeVisitor;
import org.neo4j.unsafe.impl.batchimport.staging.ProducerStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static java.lang.System.nanoTime;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;

/**
 * Using the {@link NodeRelationshipCache} efficiently looks for changed nodes and reads those
 * {@link NodeRecord} and sends downwards.
 */
public class ReadNodeIdsByCacheStep extends ProducerStep
{
    private final int nodeTypes;
    private final NodeRelationshipCache cache;
    private volatile long highId;

    public ReadNodeIdsByCacheStep( StageControl control, Configuration config,
            NodeRelationshipCache cache, int nodeTypes )
    {
        super( control, config );
        this.cache = cache;
        this.nodeTypes = nodeTypes;
    }

    @Override
    protected void process()
    {
        try ( NodeVisitor visitor = new NodeVisitor() )
        {
            cache.visitChangedNodes( visitor, nodeTypes );
        }
    }

    private class NodeVisitor implements NodeChangeVisitor, AutoCloseable
    {
        private long[] batch = new long[batchSize];
        private int cursor;
        private long time = nanoTime();

        @Override
        public void change( long nodeId, ByteArray array )
        {
            batch[cursor++] = nodeId;
            if ( cursor == batchSize )
            {
                send();
                batch = new long[batchSize];
                cursor = 0;
            }
        }

        private void send()
        {
            totalProcessingTime.add( nanoTime() - time );
            sendDownstream( iterator( batch ) );
            time = nanoTime();
            assertHealthy();
            highId = batch[cursor - 1];
        }

        @Override
        public void close()
        {
            if ( cursor > 0 )
            {
                batch = Arrays.copyOf( batch, cursor );
                send();
            }
        }
    }

    @Override
    protected long position()
    {
        return highId * Long.BYTES;
    }
}
