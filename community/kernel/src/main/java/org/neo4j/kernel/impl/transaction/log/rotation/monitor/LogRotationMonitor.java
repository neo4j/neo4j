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
package org.neo4j.kernel.impl.transaction.log.rotation.monitor;

import java.nio.file.Path;

public interface LogRotationMonitor {
    /**
     * Called when the log file is initially opened, to allow monitor callback to know which version of the file was initially opened.
     * Then later on this version will be rotated into new versions, at which point {@link #startRotation(long)} will be called.
     * @param logFile the file.
     * @param logVersion the version of the file is used when starting.
     */
    void started(Path logFile, long logVersion);

    void startRotation(long currentLogVersion);

    void finishLogRotation(
            Path logFile, long logVersion, long lastAppendIndex, long rotationMillis, long millisSinceLastRotation);
}
