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
package org.neo4j.kernel.impl.api;

import java.io.IOException;
import java.util.Arrays;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * Sometimes a test just needs a command, no particular command, just a command... this could be that command.
 */
public class TestCommand implements StorageCommand {
    private static final KernelVersion LATEST_VERSION = KernelVersion.getLatestVersion(Config.defaults());
    private final byte[] bytes;
    private final KernelVersion kernelVersion;

    public TestCommand() {
        this(LATEST_VERSION);
    }

    public TestCommand(KernelVersion kernelVersion) {
        this(50 /* roughly the size of a NodeCommand */, kernelVersion);
    }

    public TestCommand(int size) {
        this(size, LATEST_VERSION);
    }

    public TestCommand(int size, KernelVersion kernelVersion) {
        this(new byte[size], kernelVersion);
    }

    public TestCommand(byte[] bytes) {
        this(bytes, LATEST_VERSION);
    }

    public TestCommand(byte[] bytes, KernelVersion kernelVersion) {
        this.bytes = bytes;
        this.kernelVersion = kernelVersion;
    }

    @Override
    public void serialize(WritableChannel channel) throws IOException {
        channel.putInt(bytes.length);
        channel.put(bytes, bytes.length);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestCommand that = (TestCommand) o;
        return Arrays.equals(bytes, that.bytes);
    }

    @Override
    public KernelVersion kernelVersion() {
        return kernelVersion;
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
