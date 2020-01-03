/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.batchimport.stats;

import org.neo4j.internal.batchimport.staging.Step;

/**
 * Provides common {@link Stat statistics} about a {@link Step}, stats like number of processed batches,
 * processing time a.s.o.
 */
public class ProcessingStats extends GenericStatsProvider
{
    public ProcessingStats(
            long receivedBatches, long doneBatches,
            long totalProcessingTime, long average,
            long upstreamIdleTime, long downstreamIdleTime )
    {
        add( Keys.received_batches, Stats.longStat( receivedBatches ) );
        add( Keys.done_batches, Stats.longStat( doneBatches ) );
        add( Keys.total_processing_time, Stats.longStat( totalProcessingTime ) );
        add( Keys.upstream_idle_time, Stats.longStat( upstreamIdleTime ) );
        add( Keys.downstream_idle_time, Stats.longStat( downstreamIdleTime ) );
        add( Keys.avg_processing_time, Stats.longStat( average ) );
    }
}
