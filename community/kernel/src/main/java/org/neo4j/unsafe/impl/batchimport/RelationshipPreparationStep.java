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

import java.util.Arrays;

import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * Prepares {@link InputRelationship}, or at least potential slow parts of it, namely {@link IdMapper} lookup.
 * This step is also parallelizable so if it becomes a bottleneck then more processors will automatically
 * be assigned to it.
 */
public class RelationshipPreparationStep extends ProcessorStep<Batch<InputRelationship,RelationshipRecord>>
{
    private final IdMapper idMapper;

    public RelationshipPreparationStep( StageControl control, Configuration config, IdMapper idMapper )
    {
        super( control, "PREPARE", config, 0 );
        this.idMapper = idMapper;
    }

    @Override
    protected void process( Batch<InputRelationship,RelationshipRecord> batch, BatchSender sender )
    {
        InputRelationship[] input = batch.input;
        long[] ids = batch.ids = new long[input.length*2];
        for ( int i = 0; i < input.length; i++ )
        {
            InputRelationship batchRelationship = input[i];
            ids[i*2] = idMapper.get( batchRelationship.startNode(), batchRelationship.startNodeGroup() );
            ids[i*2+1] = idMapper.get( batchRelationship.endNode(), batchRelationship.endNodeGroup() );
        }
        batch.sortedIds = ids.clone();
        Arrays.sort( batch.sortedIds );
        sender.send( batch );
    }
}
