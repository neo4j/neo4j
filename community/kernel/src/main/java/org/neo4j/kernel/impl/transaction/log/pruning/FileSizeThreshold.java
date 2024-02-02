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
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.LogFileInformation;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

public final class FileSizeThreshold implements Threshold {
    private final FileSystemAbstraction fileSystem;
    private final long maxSize;
    private final InternalLog log;

    private long currentSize;

    FileSizeThreshold(FileSystemAbstraction fileSystem, long maxSize, InternalLogProvider logProvider) {
        this.fileSystem = fileSystem;
        this.maxSize = maxSize;
        this.log = logProvider.getLog(getClass());
    }

    @Override
    public void init() {
        currentSize = 0;
    }

    @Override
    public boolean reached(Path file, long version, LogFileInformation source) {
        try {
            currentSize += fileSystem.getFileSize(file);
        } catch (IOException e) {
            log.warn("Error on attempt to get file size from transaction log files. Checked version: " + version, e);
        }
        return currentSize >= maxSize;
    }

    @Override
    public String toString() {
        return maxSize + " size";
    }
}
