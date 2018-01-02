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
package org.neo4j.consistency.checking.cache;

import org.neo4j.consistency.checking.full.ConsistencyCheckerTask;
import org.neo4j.consistency.checking.full.Stage;
import org.neo4j.consistency.checking.full.StoreProcessor;
import org.neo4j.consistency.statistics.Statistics;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;

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
        private final Iterable<NodeRecord> nodes;

        public CacheNextRel( Stage stage, CacheAccess cacheAccess, Iterable<NodeRecord> nodes )
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
            for ( NodeRecord node : nodes )
            {
                if ( node.inUse() )
                {
                    fields[CacheSlots.NextRelationhip.SLOT_RELATIONSHIP_ID] = node.getNextRel();
                    client.putToCache( node.getId(), fields );
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
            for ( long nodeId = 0; nodeId < nodeStore.getHighId(); nodeId++ )
            {
                if ( client.getFromCache( nodeId, CacheSlots.NextRelationhip.SLOT_FIRST_IN_TARGET ) == 0 )
                {
                    NodeRecord node = nodeStore.forceGetRecord( nodeId );
                    if ( !node.isDense() )
                    {
                        storeProcessor.processNode( nodeStore, node );
                    }
                }
            }
        }
    }
}
