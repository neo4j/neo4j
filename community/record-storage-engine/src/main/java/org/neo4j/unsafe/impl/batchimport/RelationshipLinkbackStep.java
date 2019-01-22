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

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper.ID_NOT_FOUND;

/**
 * Links relationship chains together, the "prev" pointers of them. "next" pointers are set when
 * initially creating the relationship records. Setting prev pointers at that time would incur
 * random access and so that is done here separately with help from {@link NodeRelationshipCache}.
 */
public class RelationshipLinkbackStep extends RelationshipLinkStep
{
    public RelationshipLinkbackStep( StageControl control, Configuration config,
            NodeRelationshipCache cache, Predicate<RelationshipRecord> filter, int nodeTypes,
            StatsProvider... additionalStatsProvider )
    {
        super( control, config, cache, filter, nodeTypes, false, additionalStatsProvider );
    }

    @Override
    protected void linkStart( RelationshipRecord record )
    {
        int typeId = record.getType();
        long firstPrevRel = cache.getAndPutRelationship( record.getFirstNode(),
                typeId, Direction.OUTGOING, record.getId(), false );
        if ( firstPrevRel == ID_NOT_FOUND )
        {   // First one
            record.setFirstInFirstChain( true );
            firstPrevRel = cache.getCount( record.getFirstNode(), typeId, Direction.OUTGOING );
        }
        record.setFirstPrevRel( firstPrevRel );
    }

    @Override
    protected void linkEnd( RelationshipRecord record )
    {
        int typeId = record.getType();
        long secondPrevRel = cache.getAndPutRelationship( record.getSecondNode(),
                typeId, Direction.INCOMING, record.getId(), false );
        if ( secondPrevRel == ID_NOT_FOUND )
        {   // First one
            record.setFirstInSecondChain( true );
            secondPrevRel = cache.getCount( record.getSecondNode(), typeId, Direction.INCOMING );
        }
        record.setSecondPrevRel( secondPrevRel );
    }

    @Override
    protected void linkLoop( RelationshipRecord record )
    {
        int typeId = record.getType();
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
}
