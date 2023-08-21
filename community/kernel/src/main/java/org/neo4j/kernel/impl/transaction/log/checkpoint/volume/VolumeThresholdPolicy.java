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
package org.neo4j.kernel.impl.transaction.log.checkpoint.volume;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThreshold;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointThresholdPolicy;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.time.SystemNanoClock;

@ServiceProvider
public class VolumeThresholdPolicy implements CheckPointThresholdPolicy {
    @Override
    public String getName() {
        return "volume";
    }

    @Override
    public CheckPointThreshold createThreshold(
            Config config, SystemNanoClock clock, LogPruning logPruning, InternalLogProvider logProvider) {
        long checkpointIntervalVolume = config.get(GraphDatabaseSettings.check_point_interval_volume);
        long logFileSize = config.get(GraphDatabaseSettings.logical_log_rotation_threshold);
        return new VolumeCheckPointThreshold(checkpointIntervalVolume, logFileSize);
    }
}
