/**
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
package org.neo4j.unsafe.impl.batchimport;

import java.util.List;

import org.neo4j.helpers.Pair;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutorServiceStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Prepares {@link InputRelationship}, or at least potential slow parts of it, namely {@link IdMapper} lookup.
 * This step is also parallelizable so if it becomes a bottleneck then more processors will automatically
 * be assigned to it.
 */
public class RelationshipPreparationStep extends ExecutorServiceStep<List<InputRelationship>>
{
    private final IdMapper idMapper;

    public RelationshipPreparationStep( StageControl control, Configuration config, IdMapper idMapper )
    {
        super( control, "PREPARE", config.workAheadSize(), config.movingAverageSize(), 1, true );
        this.idMapper = idMapper;
    }

    @Override
    protected Object process( long ticket, List<InputRelationship> batch )
    {
        long[] ids = new long[batch.size()*2];
        int index = 0;
        for ( InputRelationship batchRelationship : batch )
        {
            ids[index++] = idMapper.get( batchRelationship.startNode(), batchRelationship.startNodeGroups() );
            ids[index++] = idMapper.get( batchRelationship.endNode(), batchRelationship.endNodeGroups() );
        }
        return Pair.of( batch, ids );
    }
}
