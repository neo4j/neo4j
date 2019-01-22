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

import java.util.function.Predicate;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

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
    private final RelationshipLinkingProgress progress;

    public RelationshipLinkStep( StageControl control, Configuration config,
            NodeRelationshipCache cache, Predicate<RelationshipRecord> filter, int nodeTypes, boolean forwards,
            StatsProvider... additionalStatsProvider )
    {
        super( control, "LINK", config, additionalStatsProvider );
        this.cache = cache;
        this.filter = filter;
        this.nodeTypes = nodeTypes;
        this.forwards = forwards;
        this.progress = findLinkingProgressStatsProvider();
    }

    /**
     * There should be a {@link RelationshipLinkingProgress} injected from the outside to better keep track of global
     * progress of relationship linking even when linking in multiple passes.
     */
    private RelationshipLinkingProgress findLinkingProgressStatsProvider()
    {
        for ( StatsProvider provider : additionalStatsProvider )
        {
            if ( provider instanceof RelationshipLinkingProgress )
            {
                return (RelationshipLinkingProgress) provider;
            }
        }
        return new RelationshipLinkingProgress();
    }

    @Override
    protected void forkedProcess( int id, int processors, RelationshipRecord[] batch )
    {
        int stride = forwards ? 1 : -1;
        int start = forwards ? 0 : batch.length - 1;
        int end = forwards ? batch.length : -1;
        int localChangeCount = 0;
        for ( int i = start; i != end; i += stride )
        {
            RelationshipRecord item = batch[i];
            if ( item != null && item.inUse() )
            {
                int changeCount = process( item, id, processors );
                if ( changeCount == -1 )
                {
                    // No change for this record, it's OK, all the processors will reach the same conclusion
                    batch[i].setInUse( false );
                }
                else
                {
                    localChangeCount += changeCount;
                }
            }
        }
        progress.add( localChangeCount );
    }

    public int process( RelationshipRecord record, int id, int processors )
    {
        long startNode = record.getFirstNode();
        long endNode = record.getSecondNode();
        boolean processFirst = startNode % processors == id;
        boolean processSecond = endNode % processors == id;
        int changeCount = 0;
        if ( !processFirst && !processSecond )
        {
            // We won't process this relationship, but we cannot return false because that means
            // that it won't even be updated. Arriving here merely means that this thread won't process
            // this record at all and so we won't even have to ask cache about dense or not (which is costly)
            return changeCount;
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
                    changeCount += 2;
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
                    changeCount++;
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
                    changeCount++;
                }
                changed = true;
            }
        }

        return changed ? changeCount : -1;
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
