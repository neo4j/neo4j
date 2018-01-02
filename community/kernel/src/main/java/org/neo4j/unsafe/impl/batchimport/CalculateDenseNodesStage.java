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

import java.io.IOException;

import org.neo4j.unsafe.impl.batchimport.cache.NodeRelationshipCache;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputRelationship;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;

/**
 * Counts number of relationships per node that is going to be imported by {@link RelationshipStage} later.
 * Dense node threshold is calculated based on these counts, so that correct relationship representation can be written
 * per node.
 */
public class CalculateDenseNodesStage extends Stage
{
    public CalculateDenseNodesStage( Configuration config, InputIterable<InputRelationship> relationships,
            NodeRelationshipCache cache, IdMapper idMapper,
            Collector badCollector, InputCache inputCache ) throws IOException
    {
        super( "Calculate dense nodes", config );
        add( new InputIteratorBatcherStep<>( control(), config,
                relationships.iterator(), InputRelationship.class ) );
        if ( !relationships.supportsMultiplePasses() )
        {
            add( new InputEntityCacherStep<>( control(), config, inputCache.cacheRelationships() ) );
        }
        add( new RelationshipPreparationStep( control(), config, idMapper ) );
        add( new CalculateDenseNodePrepareStep( control(), config, badCollector ) );
        add( new CalculateDenseNodesStep( control(), config, cache ) );
    }
}
