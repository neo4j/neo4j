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
package org.neo4j.io.fs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.neo4j.io.memory.ScopedBuffer;

public class PhysicalFlushableLogChannel extends PhysicalFlushableChannel implements PhysicalLogChannel {

    private long appendStartPosition;

    public PhysicalFlushableLogChannel(StoreChannel channel, ScopedBuffer scopedBuffer) {
        super(channel, scopedBuffer);
    }

    public void setChannel(StoreChannel channel) {
        this.channel = channel;
    }

    @Override
    public void resetAppendedBytesCounter() {
        try {
            appendStartPosition = position();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getAppendedBytes() {
        try {
            return position() - appendStartPosition;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public FlushableChannel put(byte[] value, int length) throws IOException {
        return super.put(value, length);
    }

    @Override
    public PhysicalFlushableChannel putVersion(byte version) throws IOException {
        return super.putVersion(version);
    }

    @Override
    public PhysicalFlushableLogChannel putAll(ByteBuffer src) throws IOException {
        super.putAll(src);
        return this;
    }
}
