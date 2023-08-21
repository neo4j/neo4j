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
package org.neo4j.test.limited;

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import org.neo4j.io.fs.DelegatingFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

public class LimitedFilesystemAbstraction extends DelegatingFileSystemAbstraction {
    private volatile boolean outOfSpace;

    public LimitedFilesystemAbstraction(FileSystemAbstraction delegate) {
        super(delegate);
    }

    @Override
    public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
        return new LimitedFileChannel(super.open(fileName, options), this);
    }

    @Override
    public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
        return new ChannelOutputStream(write(fileName), append, INSTANCE);
    }

    @Override
    public InputStream openAsInputStream(Path fileName) throws IOException {
        return new ChannelInputStream(read(fileName), INSTANCE);
    }

    @Override
    public StoreChannel write(Path fileName) throws IOException {
        ensureHasSpace();
        return new LimitedFileChannel(super.write(fileName), this);
    }

    @Override
    public void mkdirs(Path fileName) throws IOException {
        ensureHasSpace();
        super.mkdirs(fileName);
    }

    @Override
    public void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        ensureHasSpace();
        super.renameFile(from, to, copyOptions);
    }

    public void runOutOfDiskSpace(boolean outOfSpace) {
        this.outOfSpace = outOfSpace;
    }

    public void ensureHasSpace() throws IOException {
        if (outOfSpace) {
            throw new IOException("No space left on device");
        }
    }
}
