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
package org.neo4j.io.state;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.InputStreamReadableChannel;
import org.neo4j.io.fs.OutputStreamWritableChannel;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.io.marshal.EndOfStreamException;

public class SimpleFileStorage<T> implements SimpleStorage<T> {
    private final FileSystemAbstraction fileSystem;
    private final ChannelMarshal<T> marshal;
    private final Path path;

    public SimpleFileStorage(FileSystemAbstraction fileSystem, Path path, ChannelMarshal<T> marshal) {
        this.fileSystem = fileSystem;
        this.path = path;
        this.marshal = marshal;
    }

    @Override
    public boolean exists() {
        return fileSystem.fileExists(path);
    }

    @Override
    public T readState() throws IOException {
        try (ReadableChannel channel = new InputStreamReadableChannel(fileSystem.openAsInputStream(path))) {
            return marshal.unmarshal(channel);
        } catch (EndOfStreamException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void writeState(T state) throws IOException {
        if (path.getParent() != null) {
            fileSystem.mkdirs(path.getParent());
        }
        if (fileSystem.fileExists(path)) {
            fileSystem.deleteFile(path);
        }
        try (WritableChannel channel = new OutputStreamWritableChannel(fileSystem.openAsOutputStream(path, false))) {
            marshal.marshal(state, channel);
        }
    }

    @Override
    public void removeState() throws IOException {
        if (exists()) {
            fileSystem.deleteFile(path);
        }
    }
}
