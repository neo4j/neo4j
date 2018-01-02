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

import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;

/**
 * Sets the {@link NodeRecord#setNextRel(long) relationship field} on sparse nodes.
 * This is done after all sparse node relationship links have been done and the {@link NodeRelationshipCache node cache}
 * points to the first relationship for sparse each node.
 */
public class SparseNodeFirstRelationshipProcessor implements RecordProcessor<NodeRecord>
{
    private final NodeRelationshipCache cache;

    public SparseNodeFirstRelationshipProcessor( NodeRelationshipCache cache )
    {
        this.cache = cache;
    }

    @Override
    public boolean process( NodeRecord node )
    {
        long nodeId = node.getId();
        long firstRel = cache.getFirstRel( nodeId, NodeRelationshipCache.NO_GROUP_VISITOR );
        if ( firstRel != -1 )
        {
            node.setNextRel( firstRel );
        }
        return true;
    }

    @Override
    public void done()
    {   // Nothing to do here
    }
}
