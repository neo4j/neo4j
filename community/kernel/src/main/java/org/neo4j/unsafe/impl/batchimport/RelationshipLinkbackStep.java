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

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.NodeType;
import org.neo4j.unsafe.impl.batchimport.staging.ForkedProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

/**
 * Links relationship chains together, the "prev" pointers of them. "next" pointers are set when
 * initially creating the relationship records. Setting prev pointers at that time would incur
 * random access and so that is done here separately with help from {@link NodeRelationshipCache}.
 */
public class RelationshipLinkbackStep extends ForkedProcessorStep<RelationshipRecord[]>
{
    private final NodeRelationshipCache cache;
    private final int nodeTypes;

    public RelationshipLinkbackStep( StageControl control, Configuration config,
            NodeRelationshipCache cache, int nodeTypes )
    {
        super( control, "LINK", config, 0 );
        this.cache = cache;
        this.nodeTypes = nodeTypes;
    }

    @Override
    protected void forkedProcess( int id, int processors, RelationshipRecord[] batch )
    {
        for ( int i = batch.length - 1; i >= 0; i-- )
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
        boolean processFirst = record.getFirstNode() % processors == id;
        boolean processSecond = record.getSecondNode() % processors == id;
        if ( !processFirst && !processSecond )
        {
            // We won't process this relationship, but we cannot return false because that means
            // that it won't even be updated. Arriving here merely means that this thread won't process
            // this record at all and so we won't even have to ask cache about dense or not (which is costly)
            return true;
        }

        boolean firstIsDense = cache.isDense( record.getFirstNode() );
        boolean changed = false;
        boolean isLoop = record.getFirstNode() == record.getSecondNode();
        int typeId = record.getType();
        if ( isLoop )
        {
            if ( NodeType.matchesDense( nodeTypes, firstIsDense ) )
            {
                if ( processFirst )
                {
                    long prevRel = cache.getAndPutRelationship( record.getFirstNode(),
                            typeId, Direction.BOTH, record.getId(), false );
                    if ( prevRel == ID_NOT_FOUND )
                    {   // First one
                        record.setFirstInFirstChain( true );
                        record.setFirstInSecondChain( true );
                        prevRel = cache.getCount( record.getFirstNode(), typeId, Direction.BOTH );
                    }
                    record.setFirstPrevRel( prevRel );
                    record.setSecondPrevRel( prevRel );
                }
                changed = true;
            }
        }
        else
        {
            // Start node
            if ( NodeType.matchesDense( nodeTypes, firstIsDense ) )
            {
                if ( processFirst )
                {
                    long firstPrevRel = cache.getAndPutRelationship( record.getFirstNode(),
                            typeId, Direction.OUTGOING, record.getId(), false );
                    if ( firstPrevRel == ID_NOT_FOUND )
                    {   // First one
                        record.setFirstInFirstChain( true );
                        firstPrevRel = cache.getCount( record.getFirstNode(), typeId, Direction.OUTGOING );
                    }
                    record.setFirstPrevRel( firstPrevRel );
                }
                changed = true;
            }

            // End node
            boolean secondIsDense = cache.isDense( record.getSecondNode() );
            if ( NodeType.matchesDense( nodeTypes, secondIsDense ) )
            {
                if ( processSecond )
                {
                    long secondPrevRel = cache.getAndPutRelationship( record.getSecondNode(),
                            typeId, Direction.INCOMING, record.getId(), false );
                    if ( secondPrevRel == ID_NOT_FOUND )
                    {   // First one
                        record.setFirstInSecondChain( true );
                        secondPrevRel = cache.getCount( record.getSecondNode(), typeId, Direction.INCOMING );
                    }
                    record.setSecondPrevRel( secondPrevRel );
                }
                changed = true;
            }
        }

        return changed;
    }
}
