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
import org.neo4j.kernel.KernelVersion;

public interface RotatableFile {
    /**
     * @return {@code true} if a rotation is needed.
     */
    boolean rotationNeeded() throws IOException;

    /**
     * Rotate the active file.
     * @return A file object representing the file name and path of the log file rotated to.
     * @throws IOException if something goes wrong with either flushing the existing log file, or creating the new log file.
     */
    Path rotate() throws IOException;

    /**
     * Rotate the active file but be explicit about what values to use for the new header.
     * This can be necessary in scenarios where the kernel version repository is not
     * updated (because apply to store hasn't happened yet), or the transaction id sequence has already been updated but
     * this rotation takes place before writing the new transaction.
     *
     * @return A file object representing the file name and path of the log file rotated to.
     * @throws IOException if something goes wrong with either flushing the existing log file, or creating the new log file.
     */
    Path rotate(KernelVersion kernelVersion, long lastAppendIndex, int checksum) throws IOException;

    long rotationSize();
}
