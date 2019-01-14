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
package org.neo4j.consistency.checking.cache;

import org.neo4j.consistency.checking.full.ConsistencyCheckerTask;
import org.neo4j.consistency.checking.full.Stage;
import org.neo4j.consistency.checking.full.StoreProcessor;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;

import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

/**
 * Action to be manipulate the {@link CacheAccess} in some way.
 */
public abstract class CacheTask extends ConsistencyCheckerTask
{
    protected final Stage stage;
    protected final CacheAccess cacheAccess;

    public CacheTask( Stage stage, CacheAccess cacheAccess )
    {
        super( "CacheTask-" + stage, Statistics.NONE, 1 );
        this.stage = stage;
        this.cacheAccess = cacheAccess;
    }

    @Override
    public void run()
    {
        if ( stage.getCacheSlotSizes().length > 0 )
        {
            cacheAccess.setCacheSlotSizes( stage.getCacheSlotSizes() );
        }
        processCache();
    }

    protected abstract void processCache();

    public static class CacheNextRel extends CacheTask
    {
        private final ResourceIterable<NodeRecord> nodes;

        public CacheNextRel( Stage stage, CacheAccess cacheAccess, ResourceIterable<NodeRecord> nodes )
        {
            super( stage, cacheAccess );
            this.nodes = nodes;
        }

        @Override
        protected void processCache()
        {
            cacheAccess.clearCache();
            long[] fields = new long[] {1, 0, -1};
            CacheAccess.Client client = cacheAccess.client();
            try ( ResourceIterator<NodeRecord> nodeRecords = nodes.iterator() )
            {
                while ( nodeRecords.hasNext() )
                {
                    NodeRecord node = nodeRecords.next();
                    if ( node.inUse() )
                    {
                        fields[CacheSlots.NextRelationship.SLOT_RELATIONSHIP_ID] = node.getNextRel();
                        client.putToCache( node.getId(), fields );
                    }
                }
            }
        }
    }

    public static class CheckNextRel extends CacheTask
    {
        private final StoreAccess storeAccess;
        private final StoreProcessor storeProcessor;

        public CheckNextRel( Stage stage, CacheAccess cacheAccess, StoreAccess storeAccess,
                StoreProcessor storeProcessor )
        {
            super( stage, cacheAccess );
            this.storeAccess = storeAccess;
            this.storeProcessor = storeProcessor;
        }

        @Override
        protected void processCache()
        {
            RecordStore<NodeRecord> nodeStore = storeAccess.getNodeStore();
            CacheAccess.Client client = cacheAccess.client();
            long highId = nodeStore.getHighId();
            for ( long nodeId = 0; nodeId < highId; nodeId++ )
            {
                if ( client.getFromCache( nodeId, CacheSlots.NextRelationship.SLOT_FIRST_IN_TARGET ) == 0 )
                {
                    NodeRecord node = nodeStore.getRecord( nodeId, nodeStore.newRecord(), FORCE );
                    if ( node.inUse() && !node.isDense() )
                    {
                        storeProcessor.processNode( nodeStore, node );
                    }
                }
            }
        }
    }
}
