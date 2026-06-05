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

import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * Prunes checkpoint log files down to {@code checkpoint_logical_log_keep_threshold} files. Checkpoint logs are a
 * separate file series from the transaction logs and have their own retention rule (a fixed file count, not a
 * strategy), so the pruning logic is encapsulated here rather than tangled with tx-log pruning.
 */
class CheckpointLogFilePruner {
    private final FileSystemAbstraction fs;
    private final LogFiles logFiles;
    private final InternalLog log;
    private final int filesToKeep;

    CheckpointLogFilePruner(
            FileSystemAbstraction fs, LogFiles logFiles, InternalLogProvider logProvider, Config config) {
        this.fs = fs;
        this.logFiles = logFiles;
        this.log = logProvider.getLog(getClass());
        this.filesToKeep = config.get(checkpoint_logical_log_keep_threshold);
    }

    void prune() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointFiles = checkpointFile.getMatchedFiles();
        if (isNotEmpty(checkpointFiles) && checkpointFiles.length > filesToKeep) {
            long highestVersionToRemove = checkpointFile.getCurrentLogVersion() - filesToKeep;
            int filesDeleted = 0;
            for (Path file : checkpointFiles) {
                if (checkpointFile.getLogVersion(file) <= highestVersionToRemove) {
                    fs.deleteFile(file);
                    filesDeleted++;
                }
            }
            log.info("Pruned " + filesDeleted + " checkpoint log files. Lowest preserved version: "
                    + (highestVersionToRemove + 1));
        }
    }
}
