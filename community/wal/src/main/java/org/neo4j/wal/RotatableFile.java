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
package org.neo4j.wal;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.rotation.NextLogHeader;

public interface RotatableFile {
    /**
     * @param file       the file rotated to.
     * @param logHeader  the header of the file rotated to, as read back from disk.
     * @param nextHeader the values the rotation resolved for that header.
     */
    record RotationInfo(Path file, LogHeader logHeader, NextLogHeader nextHeader) {}

    /**
     * @return {@code true} if a rotation is needed.
     */
    boolean rotationNeeded() throws IOException;

    /**
     * Rotate the active file. The header of the new file takes the values set on {@code headerBuilder}, and any unset
     * ones from this file's live state inside the rotation critical section.
     *
     * @return A file object representing the file name and path of the log file rotated to.
     * @throws IOException if something goes wrong with either flushing the existing log file, or creating the new log file.
     */
    RotationInfo rotate(NextLogHeader.Builder headerBuilder) throws IOException;

    /**
     * Rotate the active file, with a header derived entirely from this file's live state.
     */
    default RotationInfo rotate() throws IOException {
        return rotate(NextLogHeader.builder());
    }

    /**
     * @return the version of the file currently being written, which a rotation moves away from. Only stable while
     * the caller has exclusive access to the file.
     */
    long activeFileVersion();

    long rotationSize();
}
