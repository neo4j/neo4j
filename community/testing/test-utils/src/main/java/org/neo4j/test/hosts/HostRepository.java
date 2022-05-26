/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.test.hosts;

import static org.neo4j.test.hosts.HostConstants.EPHEMERAL_HOST_MAXIMUM;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class HostRepository {
    private final Path directory;
    private long currentHost;

    HostRepository(Path directory, long initialHost) {
        this.directory = directory;
        this.currentHost = initialHost;
    }

    // synchronize between threads in this JVM
    synchronized long reserveNextHost(String trace) {
        while (currentHost <= EPHEMERAL_HOST_MAXIMUM) {
            var hostFilePath = directory.resolve("host" + Long.toHexString(currentHost));
            try {
                // synchronize between processes on this machine
                Files.createFile(hostFilePath);

                // write a trace for debugging purposes
                try (OutputStream fileOutputStream = Files.newOutputStream(hostFilePath, StandardOpenOption.APPEND)) {
                    fileOutputStream.write(trace.getBytes());
                }
                hostFilePath.toFile().deleteOnExit();
                return currentHost++;
            } catch (FileAlreadyExistsException e) {
                currentHost++;
            } catch (IOException e) {
                throw new IllegalStateException("An unexpected IOException occurred", e);
            }
        }

        throw new IllegalStateException("There are no more hosts available");
    }
}
