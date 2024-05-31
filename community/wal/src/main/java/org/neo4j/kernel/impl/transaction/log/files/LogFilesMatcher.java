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

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;

public class LogFilesMatcher {
    private final TransactionLogFilesHelper checkpointFilesHelper;
    private final TransactionLogFilesHelper transactionLogFilesHelper;

    public LogFilesMatcher(FileSystemAbstraction fileSystem, Path logFilesDirectory) {
        this.transactionLogFilesHelper = TransactionLogFilesHelper.forTransactions(fileSystem, logFilesDirectory);
        this.checkpointFilesHelper = TransactionLogFilesHelper.forCheckpoints(fileSystem, logFilesDirectory);
    }

    public Path[] getCheckpointLogFiles() throws IOException {
        return checkpointFilesHelper.getMatchedFiles();
    }

    public Path[] getTransactionLogFiles() throws IOException {
        return transactionLogFilesHelper.getMatchedFiles();
    }

    public boolean hasAnyLogFiles() {
        try {
            return getTransactionLogFiles().length > 0 || getCheckpointLogFiles().length > 0;
        } catch (IOException e) {
            // Could not check txlogs (does not exist?) Do nothing
            return false;
        }
    }

    public boolean isLogFile(Path file) {
        return transactionLogFilesHelper.isLogFile(file) || checkpointFilesHelper.isLogFile(file);
    }
}
