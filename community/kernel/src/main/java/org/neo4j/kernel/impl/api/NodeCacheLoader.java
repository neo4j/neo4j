/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import org.neo4j.kernel.impl.api.PersistenceCache.CachedNodeEntity;
import org.neo4j.kernel.impl.cache.LockStripedCache;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;

class NodeCacheLoader implements LockStripedCache.Loader<CachedNodeEntity>
{
    private final NodeStore nodeStore;

    NodeCacheLoader( NodeStore nodeStore )
    {
        this.nodeStore = nodeStore;
    }

    @Override
    public CachedNodeEntity loadById( long id )
    {
        try
        {
            CachedNodeEntity result = new CachedNodeEntity( id );
            result.addLabels( asSet(asIterable(nodeStore.getLabelsForNode(nodeStore.getRecord(id))) ) );
            return result;
        }
        catch ( InvalidRecordException e )
        {
            return null;
        }
    }
}
