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

import static org.neo4j.graphdb.Direction.BOTH;

public class RelationshipLinkforwardStep extends RelationshipLinkStep
{
    public RelationshipLinkforwardStep( StageControl control, Configuration config, NodeRelationshipCache cache,
            Predicate<RelationshipRecord> filter, int nodeTypes, StatsProvider... additionalStatsProvider )
    {
        super( control, config, cache, filter, nodeTypes, true, additionalStatsProvider );
    }

    @Override
    protected void linkStart( RelationshipRecord record )
    {
        long firstNextRel = cache.getAndPutRelationship( record.getFirstNode(),
                record.getType(), Direction.OUTGOING, record.getId(), true );
        record.setFirstNextRel( firstNextRel );
    }

    @Override
    protected void linkEnd( RelationshipRecord record )
    {
        long secondNextRel = cache.getAndPutRelationship( record.getSecondNode(),
                record.getType(), Direction.INCOMING, record.getId(), true );
        record.setSecondNextRel( secondNextRel );
    }

    @Override
    protected void linkLoop( RelationshipRecord record )
    {
        long firstNextRel = cache.getAndPutRelationship(
                record.getFirstNode(), record.getType(), BOTH, record.getId(), true );
        record.setFirstNextRel( firstNextRel );
        record.setSecondNextRel( firstNextRel );
    }
}
