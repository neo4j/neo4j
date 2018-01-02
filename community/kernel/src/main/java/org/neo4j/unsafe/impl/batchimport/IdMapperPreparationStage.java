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

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.InputCache;
import org.neo4j.unsafe.impl.batchimport.input.InputNode;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

import static org.neo4j.unsafe.impl.batchimport.Utils.idsOf;

/**
 * Performs {@link IdMapper#prepare(InputIterable, Collector, ProgressListener)}
 * embedded in a {@link Stage} as to take advantage of statistics and monitoring provided by that framework.
 */
public class IdMapperPreparationStage extends Stage
{
    public IdMapperPreparationStage( Configuration config, IdMapper idMapper, InputIterable<InputNode> nodes,
            InputCache inputCache, Collector collector, StatsProvider memoryUsageStats )
    {
        super( "Prepare node index", config );
        add( new IdMapperPreparationStep( control(), config,
                idMapper, idsOf( nodes.supportsMultiplePasses() ? nodes : inputCache.nodes() ),
                collector, memoryUsageStats ) );
    }
}
