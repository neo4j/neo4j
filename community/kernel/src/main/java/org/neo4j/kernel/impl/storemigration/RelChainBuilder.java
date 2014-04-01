/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

/**
 * Allows incrementally building up a relationship chain, and allows telling when the chain is complete.
 */
public class RelChainBuilder implements Iterable<RelationshipRecord>
{
    static class ChainEntry
    {
        private final RelationshipRecord record;
        private final boolean isLast;
        private ChainEntry next;

        ChainEntry( RelationshipRecord record, boolean isLast, ChainEntry next )
        {
            this.record = record;
            this.isLast = isLast;
            this.next = next;
        }
    }

    private final long nodeId;

    /**
     * Makes up a singly linked list of relationships, will only contain the parts that are complete starting from
     * the first rel. */
    private ChainEntry head = null;

    /** Fast lookup of relationships in the chain by id. */
    private Map<Long, ChainEntry> chainIndex = new HashMap<>();

    public RelChainBuilder( long nodeId )
    {
        this.nodeId = nodeId;
    }

    public void append( RelationshipRecord record, long prevRel, long nextRel )
    {
        boolean isFirst = prevRel == Record.NO_PREV_RELATIONSHIP.intValue();
        boolean isLast = nextRel == Record.NO_NEXT_RELATIONSHIP.intValue();

        ChainEntry entry = new ChainEntry( record, isLast, chainIndex.get( nextRel ) );
        chainIndex.put( record.getId(), entry );

        if(isFirst)
        {
            head = entry;
        }
        else
        {
            ChainEntry prevInChain = chainIndex.get( prevRel );
            if(prevInChain != null)
            {
                prevInChain.next = entry;

            }
        }
    }

    public boolean isComplete()
    {
        for(ChainEntry entry = head; entry != null; entry = entry.next)
            if(entry.isLast) return true;
        return false;
    }

    public int size()
    {
        return chainIndex.size();
    }

    public long nodeId()
    {
        return nodeId;
    }

    @Override
    public Iterator<RelationshipRecord> iterator()
    {
        return new Iterator<RelationshipRecord>()
        {
            private ChainEntry next = head;

            @Override
            public boolean hasNext()
            {
                return next != null;
            }

            @Override
            public RelationshipRecord next()
            {
                ChainEntry current = next;
                next = current.next;
                return current.record;
            }

            @Override
            public void remove()
            {

            }
        };
    }
}
