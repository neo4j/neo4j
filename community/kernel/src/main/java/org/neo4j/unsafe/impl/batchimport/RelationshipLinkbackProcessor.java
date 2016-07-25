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

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;

/**
 * Links the {@code previous} fields in {@link RelationshipRecord relationship records}. This is done after
 * a forward pass where the {@code next} fields are linked.
 */
public class RelationshipLinkbackProcessor implements RecordProcessor<RelationshipRecord>
{
    private final NodeRelationshipCache cache;
    private final boolean denseNodes;

    public RelationshipLinkbackProcessor( NodeRelationshipCache cache, boolean denseNodes )
    {
        this.cache = cache;
        this.denseNodes = denseNodes;
    }

    @Override
    public boolean process( RelationshipRecord record )
    {
        boolean isLoop = record.getFirstNode() == record.getSecondNode();
        boolean firstIsDense = cache.isDense( record.getFirstNode() );
        boolean changed = false;
        if ( isLoop )
        {
            if ( firstIsDense == denseNodes )
            {
                long prevRel = cache.getAndPutRelationship( record.getFirstNode(),
                        Direction.BOTH, record.getId(), false );
                if ( prevRel == -1 )
                {   // First one
                    record.setFirstInFirstChain( true );
                    record.setFirstInSecondChain( true );
                    prevRel = cache.getCount( record.getFirstNode(), Direction.BOTH );
                }
                record.setFirstPrevRel( prevRel );
                record.setSecondPrevRel( prevRel );
                changed = true;
            }
        }
        else
        {
            // Start node
            if ( firstIsDense == denseNodes )
            {
                long firstPrevRel = cache.getAndPutRelationship( record.getFirstNode(),
                        Direction.OUTGOING, record.getId(), false );
                if ( firstPrevRel == -1 )
                {   // First one
                    record.setFirstInFirstChain( true );
                    firstPrevRel = cache.getCount( record.getFirstNode(), Direction.OUTGOING );
                }
                record.setFirstPrevRel( firstPrevRel );
                changed = true;
            }

            // End node
            boolean secondIsDense = cache.isDense( record.getSecondNode() );
            if ( secondIsDense == denseNodes )
            {
                long secondPrevRel = cache.getAndPutRelationship( record.getSecondNode(),
                        Direction.INCOMING, record.getId(), false );
                if ( secondPrevRel == -1 )
                {   // First one
                    record.setFirstInSecondChain( true );
                    secondPrevRel = cache.getCount( record.getSecondNode(), Direction.INCOMING );
                }
                record.setSecondPrevRel( secondPrevRel );
                changed = true;
            }
        }
        return changed;
    }

    @Override
    public void done()
    {   // Nothing to do here
    }
}
