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

import java.util.function.LongFunction;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.staging.Stage;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

/**
 * Performs {@link IdMapper#prepare(LongFunction, Collector, ProgressListener)}
 * embedded in a {@link Stage} as to take advantage of statistics and monitoring provided by that framework.
 */
public class IdMapperPreparationStage extends Stage
{
    public static final String NAME = "Prepare node index";

    public IdMapperPreparationStage( Configuration config, IdMapper idMapper, LongFunction<Object> inputIdLookup,
            Collector collector, StatsProvider memoryUsageStats )
    {
        super( NAME, null, config, 0 );
        add( new IdMapperPreparationStep( control(), config, idMapper, inputIdLookup, collector, memoryUsageStats ) );
    }
}
