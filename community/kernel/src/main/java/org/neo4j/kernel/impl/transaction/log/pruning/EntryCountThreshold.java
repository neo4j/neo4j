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

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public final class EntryCountThreshold implements Threshold {
    private final long maxLogEntries;
    private final InternalLog log;

    EntryCountThreshold(InternalLogProvider logProvider, long maxLogEntries) {
        this.log = logProvider.getLog(getClass());
        this.maxLogEntries = maxLogEntries;
    }

    @Override
    public void init() {
        // nothing to do here
    }

    @Override
    public boolean reached(Path ignored, long version, LogFileInformation source) {
        try {
            long lastAppendIndex = source.getFirstEntryAppendIndex(version);
            if (lastAppendIndex == -1) {
                log.warn(
                        "Failed to get append index of the first entry in the transaction log file. Requested version: "
                                + version);
                return false;
            }

            long highestAppendIndex = source.getLastEntryAppendIndex();
            return highestAppendIndex - lastAppendIndex >= maxLogEntries;
        } catch (IOException e) {
            log.warn(
                    "Error on attempt to get entry append indexes from transaction log files. Checked version: "
                            + version,
                    e);
            return false;
        }
    }

    @Override
    public String toString() {
        return maxLogEntries + " entries";
    }
}
