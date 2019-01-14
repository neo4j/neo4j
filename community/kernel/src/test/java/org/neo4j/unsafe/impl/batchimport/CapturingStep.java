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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;
import org.neo4j.unsafe.impl.batchimport.stats.StatsProvider;

public class CapturingStep<T> extends ProcessorStep<T>
{
    private final List<T> receivedBatches = Collections.synchronizedList( new ArrayList<>() );

    public CapturingStep( StageControl control, String name, Configuration config,
            StatsProvider... additionalStatsProvider )
    {
        super( control, name, config, 1, additionalStatsProvider );
    }

    public List<T> receivedBatches()
    {
        return receivedBatches;
    }

    @Override
    protected void process( T batch, BatchSender sender )
    {
        receivedBatches.add( batch );
    }
}
