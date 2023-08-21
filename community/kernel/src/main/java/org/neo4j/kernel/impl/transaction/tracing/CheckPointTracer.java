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
package org.neo4j.kernel.impl.transaction.tracing;

import org.neo4j.kernel.impl.transaction.stats.CheckpointCounters;

public interface CheckPointTracer extends CheckpointCounters {
    CheckPointTracer NULL = new CheckPointTracer() {
        @Override
        public LogCheckPointEvent beginCheckPoint() {
            return LogCheckPointEvent.NULL;
        }

        @Override
        public long numberOfCheckPoints() {
            return 0;
        }

        @Override
        public long checkPointAccumulatedTotalTimeMillis() {
            return 0;
        }

        @Override
        public long lastCheckpointTimeMillis() {
            return 0;
        }

        @Override
        public long lastCheckpointPagesFlushed() {
            return 0;
        }

        @Override
        public long lastCheckpointIOs() {
            return 0;
        }

        @Override
        public long lastCheckpointIOLimit() {
            return 0;
        }

        @Override
        public long lastCheckpointIOLimitedTimes() {
            return 0;
        }

        @Override
        public long lastCheckpointIOLimitedMillis() {
            return 0;
        }

        @Override
        public long flushedBytes() {
            return 0;
        }
    };

    /**
     * Begin a check point write to the log
     */
    LogCheckPointEvent beginCheckPoint();
}
