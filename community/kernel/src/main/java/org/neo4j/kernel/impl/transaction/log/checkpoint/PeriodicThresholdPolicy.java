/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold.or;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.time.SystemNanoClock;

/**
 * The {@code periodic} check point threshold policy uses the {@link GraphDatabaseSettings#check_point_interval_time}
 * and {@link GraphDatabaseSettings#check_point_interval_tx} to decide when check points processes should be started.
 */
@ServiceProvider
public class PeriodicThresholdPolicy implements CheckPointThresholdPolicy {
    @Override
    public String getName() {
        return "periodic";
    }

    @Override
    public CheckPointThreshold createThreshold(
            Config config, SystemNanoClock clock, LogPruning logPruning, InternalLogProvider logProvider) {
        int txThreshold = config.get(GraphDatabaseSettings.check_point_interval_tx);
        final CountCommittedLogChunksThreshold countCommittedLogChunksThreshold =
                new CountCommittedLogChunksThreshold(txThreshold);

        long timeMillisThreshold =
                config.get(GraphDatabaseSettings.check_point_interval_time).toMillis();
        TimeCheckPointThreshold timeCheckPointThreshold = new TimeCheckPointThreshold(timeMillisThreshold, clock);

        return or(countCommittedLogChunksThreshold, timeCheckPointThreshold);
    }
}
