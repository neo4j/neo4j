/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.function.Predicate;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Links relationship chains together, the "prev" pointers of them. "next" pointers are set when
 * initially creating the relationship records. Setting prev pointers at that time would incur
 * random access and so that is done here separately with help from {@link NodeRelationshipCache}.
 */
public abstract class RelationshipLinkStep extends ForkedProcessorStep<RelationshipRecord[]>
{
    protected final NodeRelationshipCache cache;
    private final int nodeTypes;
    private final Predicate<RelationshipRecord> filter;
    private final boolean forwards;

    public RelationshipLinkStep( StageControl control, Configuration config,
            NodeRelationshipCache cache, Predicate<RelationshipRecord> filter, int nodeTypes, boolean forwards )
    {
        super( control, "LINK", config );
        this.cache = cache;
        this.filter = filter;
        this.nodeTypes = nodeTypes;
        this.forwards = forwards;
    }

    @Override
    protected void forkedProcess( int id, int processors, RelationshipRecord[] batch )
    {
        int stride = forwards ? 1 : -1;
        int start = forwards ? 0 : batch.length - 1;
        int end = forwards ? batch.length : -1;

        for ( int i = start; i != end; i += stride )
        {
            RelationshipRecord item = batch[i];
            if ( item != null && item.inUse() )
            {
                if ( !process( item, id, processors ) )
                {
                    // No change for this record, it's OK, all the processors will reach the same conclusion
                    batch[i] = null;
                }
            }
        }
    }

    public boolean process( RelationshipRecord record, int id, int processors )
    {
        long startNode = record.getFirstNode();
        long endNode = record.getSecondNode();
        boolean processFirst = startNode % processors == id;
        boolean processSecond = endNode % processors == id;
        if ( !processFirst && !processSecond )
        {
            // We won't process this relationship, but we cannot return false because that means
            // that it won't even be updated. Arriving here merely means that this thread won't process
            // this record at all and so we won't even have to ask cache about dense or not (which is costly)
            return true;
        }

        boolean firstIsDense = cache.isDense( startNode );
        boolean changed = false;
        boolean isLoop = startNode == endNode;
        if ( isLoop )
        {
            // Both start/end node
            if ( shouldChange( firstIsDense, record ) )
            {
                if ( processFirst )
                {
                    linkLoop( record );
                }
                changed = true;
            }
        }
        else
        {
            // Start node
            if ( shouldChange( firstIsDense, record ) )
            {
                if ( processFirst )
                {
                    linkStart( record );
                }
                changed = true;
            }

            // End node
            boolean secondIsDense = cache.isDense( endNode );
            if ( shouldChange( secondIsDense, record ) )
            {
                if ( processSecond )
                {
                    linkEnd( record );
                }
                changed = true;
            }
        }

        return changed;
    }

    protected abstract void linkStart( RelationshipRecord record );

    protected abstract void linkEnd( RelationshipRecord record );

    protected abstract void linkLoop( RelationshipRecord record );

    private boolean shouldChange( boolean isDense, RelationshipRecord record )
    {
        if ( !NodeType.matchesDense( nodeTypes, isDense ) )
        {
            return false;
        }
        // Here we have a special case where we want to filter on type, but only for dense nodes
        return !(isDense && filter != null && !filter.test( record ));
    }
}
