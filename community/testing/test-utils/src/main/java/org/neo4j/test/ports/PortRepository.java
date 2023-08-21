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
package org.neo4j.test.ports;

import static org.neo4j.test.ports.PortConstants.EPHEMERAL_PORT_MAXIMUM;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class PortRepository {
    private final Path directory;

    private int currentPort;

    PortRepository(Path directory, int initialPort) {
        this.directory = directory;

        this.currentPort = initialPort;
    }

    // synchronize between threads in this JVM
    synchronized int reserveNextPort(String trace) {
        while (currentPort <= EPHEMERAL_PORT_MAXIMUM) {
            Path portFilePath = directory.resolve("port" + currentPort);

            try {
                // synchronize between processes on this machine
                Files.createFile(portFilePath);

                // write a trace for debugging purposes
                try (OutputStream fileOutputStream = Files.newOutputStream(portFilePath, StandardOpenOption.APPEND)) {
                    fileOutputStream.write(trace.getBytes());
                }

                return currentPort++;
            } catch (FileAlreadyExistsException e) {
                currentPort++;
            } catch (IOException e) {
                throw new IllegalStateException("This will never happen - LWN", e);
            }
        }

        throw new IllegalStateException("There are no more ports available");
    }
}
