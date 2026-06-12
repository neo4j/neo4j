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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import java.io.IOException;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruneThreshold;

class LogFilesPruner {
    private final LogsRepository logsRepository;
    // setThreshold may be called concurrently with a running pruneUpTo. The cycle captures its predicate at the
    // top, so a swap mid-cycle only takes effect on the next call.
    private volatile LogPruneThreshold threshold;

    LogFilesPruner(LogsRepository logsRepository, LogPruneThreshold threshold) {
        this.logsRepository = logsRepository;
        this.threshold = threshold;
    }

    void setThreshold(LogPruneThreshold threshold) {
        this.threshold = threshold;
    }

    /**
     * @param desiredVersionToPrune the highest version the caller is willing to delete; anything strictly
     *                              greater is kept regardless of threshold.
     * @param currentIndex          the live append index, snapshotted before iteration begins.
     * @return the highest version actually deleted, or {@code -1} when nothing was pruned.
     */
    long pruneUpTo(long desiredVersionToPrune, long currentIndex) throws IOException {
        var prunePredicate = threshold.forCycle(currentIndex);

        var upToVersion = desiredVersionToPrune + 1;
        long boundaryVersion = -1;
        try (var cursor = new LogFilesMetadata(logsRepository, true)) {
            while (cursor.next()) {
                var logFileMetadata = cursor.get();
                long logFileVersion = logFileMetadata.version();
                var logFileInformation = new EnvelopedLogFileInformation(
                        logFileMetadata, logsRepository.lastModifiedTime(logFileVersion));
                // Predicate runs on every file so size/time thresholds accumulate state across the whole
                // log; the horizon only gates whether we accept this version as the boundary.
                if (prunePredicate.isLowestVersionToKeep(logFileInformation) && logFileVersion <= upToVersion) {
                    boundaryVersion = logFileVersion;
                    break;
                }
            }
        }

        if (boundaryVersion == -1) {
            return -1;
        }
        var highestToPrune = boundaryVersion - 1;
        if (highestToPrune < logsRepository.logVersionsRange().from()) {
            return -1;
        }
        logsRepository.deleteLogFilesTo(highestToPrune);
        return highestToPrune;
    }
}
