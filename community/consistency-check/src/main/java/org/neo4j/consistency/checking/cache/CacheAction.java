/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.consistency.checking.full.StoreProcessor;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.StoreAccess;
import org.neo4j.kernel.impl.store.record.NodeRecord;

/**
 * Action to be run as part of a {@link CacheProcessor}.
 */
public abstract class CacheAction
{
    protected final CacheAccess cacheAccess;

    public CacheAction( CacheAccess cacheAccess )
    {
        this.cacheAccess = cacheAccess;
    }

    public abstract boolean processCache();

    public static class CacheNextRel extends CacheAction
    {
        private final Iterable<NodeRecord> nodes;

        public CacheNextRel( CacheAccess cacheAccess, Iterable<NodeRecord> nodes )
        {
            super( cacheAccess );
            this.nodes = nodes;
        }

        @Override
        public boolean processCache()
        {
            cacheAccess.clearCache();
            long[] fields = new long[] {1, 0, -1};
            CacheAccess.Client client = cacheAccess.client();
            for ( NodeRecord node : nodes )
            {
                if ( node.inUse() )
                {
                    fields[2] = node.getNextRel();
                    client.putToCache( node.getId(), fields );
                }
            }
            return true;
        }
    }

    public static class ClearCache extends CacheAction
    {
        public ClearCache( CacheAccess cacheAccess )
        {
            super( cacheAccess );
        }

        @Override
        public boolean processCache()
        {
            cacheAccess.clearCache();
            return true;
        }
    }

    public static class CheckNextRel extends CacheAction
    {
        private final StoreAccess storeAccess;
        private final StoreProcessor storeProcessor;

        public CheckNextRel( CacheAccess cacheAccess, StoreAccess storeAccess, StoreProcessor storeProcessor )
        {
            super( cacheAccess );
            this.storeAccess = storeAccess;
            this.storeProcessor = storeProcessor;
        }

        @Override
        public boolean processCache()
        {
            RecordStore<NodeRecord> nodeStore = storeAccess.getNodeStore();
            CacheAccess.Client client = cacheAccess.client();
            for ( long nodeId = 0; nodeId < nodeStore.getHighId(); nodeId++ )
            {
                if ( client.getFromCache( nodeId, 1 ) == 0 )
                {
                    NodeRecord node = nodeStore.forceGetRaw( nodeId );
                    if ( !node.isDense() )
                    {
                        storeProcessor.processNode( nodeStore, node );
                    }
                }
            }
            return true;
        }
    }
}
