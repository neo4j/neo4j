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
package org.neo4j.kernel.impl.transaction.log.files;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

public interface LogFileVersionTracker {

    LogFileVersionTracker NO_OP = new LogFileVersionTracker() {
        @Override
        public void logDeleted(long version) {}

        @Override
        public void logCompleted(LogPosition endLogPosition) {}
    };

    /**
     * @param version the version of the log file deleted, e.g. during a pruning
     */
    void logDeleted(long version);

    /**
     * @param endLogPosition the log position at the end of the log file when it was rotated.
     *                       Note that this call is <strong>AFTER</strong> the new channel is created during the
     *                       rotation.
     */
    void logCompleted(LogPosition endLogPosition);
}
