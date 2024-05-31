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
package org.neo4j.kernel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesMatcher;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

@DbmsExtension
public class LogFileMatcherIT {
    @Inject
    private GraphDatabaseAPI database;

    @Inject
    private LogFiles logFiles;

    @Inject
    private FileSystemAbstraction fs;

    @Test
    void logFileMatching() throws IOException {
        Path logsDirectory = database.databaseLayout().getTransactionLogsDirectory();

        LogFilesMatcher logFilesMatcher = new LogFilesMatcher(fs, logsDirectory);
        assertTrue(logFilesMatcher.hasAnyLogFiles());
        assertThat(logFiles.getCheckpointFile().getDetachedCheckpointFiles())
                .isEqualTo(logFilesMatcher.getCheckpointLogFiles());
        assertThat(logFiles.getLogFile().getMatchedFiles()).isEqualTo(logFilesMatcher.getTransactionLogFiles());
    }

    @Test
    void wrongDirectoryLogFileMatching() throws IOException {
        Path logsDirectory = fs.createTempDirectory(database.databaseLayout().getTransactionLogsDirectory(), "other");

        LogFilesMatcher logFilesMatcher = new LogFilesMatcher(fs, logsDirectory);
        assertFalse(logFilesMatcher.hasAnyLogFiles());
        assertThat(logFiles.getCheckpointFile().getDetachedCheckpointFiles())
                .isNotEqualTo(logFilesMatcher.getCheckpointLogFiles());
        assertThat(logFiles.getLogFile().getMatchedFiles()).isNotEqualTo(logFilesMatcher.getTransactionLogFiles());
    }

    @Test
    void individualFileMatching() throws IOException {
        Path logsDirectory = database.databaseLayout().getTransactionLogsDirectory();
        LogFilesMatcher logFilesMatcher = new LogFilesMatcher(fs, logsDirectory);

        assertFalse(logFilesMatcher.isLogFile(Path.of("foo")));
        assertFalse(logFilesMatcher.isLogFile(logsDirectory));
        assertTrue(logFilesMatcher.isLogFile(logFiles.getCheckpointFile().getCurrentFile()));
        assertTrue(logFilesMatcher.isLogFile(logFiles.getLogFile().getHighestLogFile()));
    }
}
