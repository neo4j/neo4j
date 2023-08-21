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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.neo4j.storageengine.api.LogVersionRepository.INITIAL_LOG_VERSION;

import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFileInformation;

public class ThresholdBasedPruneStrategy implements LogPruneStrategy {
    private final LogFile logFile;
    private final Threshold threshold;
    private final TransactionLogFileInformation logFileInformation;

    ThresholdBasedPruneStrategy(LogFile logFile, Threshold threshold) {
        this.logFile = logFile;
        this.logFileInformation = logFile.getLogFileInformation();
        this.threshold = threshold;
    }

    @Override
    public String toString() {
        return threshold.toString();
    }

    @Override
    public synchronized VersionRange findLogVersionsToDelete(long upToVersion) {
        if (upToVersion == INITIAL_LOG_VERSION) {
            return LogPruneStrategy.EMPTY_RANGE;
        }

        threshold.init();
        long lowestLogVersion = logFile.getLowestLogVersion();
        for (long version = upToVersion; version >= lowestLogVersion; version--) {
            if (threshold.reached(logFile.getLogFileForVersion(version), version, logFileInformation)) {
                return new VersionRange(lowestLogVersion, version);
            }
        }

        return LogPruneStrategy.EMPTY_RANGE;
    }
}
