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

import java.io.Flushable;
import java.io.IOException;

/**
 * Provides flush semantics over a {@link WritableChannel}. Essentially, this interface implies the existence of a
 * buffer over a {@link WritableChannel}, allowing for batching of writes, controlled via the {@link #prepareForFlush}
 * call.
 */
public interface FlushableChannel extends WritableChannel {
    /**
     * Ensures that all written content will be present in the file channel. This method does not flush, it prepares for
     * it, by returning a handle for flushing at a later time.
     * The returned handle guarantees that the writes that happened before its creation will be flushed. Implementations
     * may choose to flush content that was written after a call to this method was made.
     */
    Flushable prepareForFlush() throws IOException;

    @Override
    FlushableChannel put(byte value) throws IOException;

    @Override
    FlushableChannel putShort(short value) throws IOException;

    @Override
    FlushableChannel putInt(int value) throws IOException;

    @Override
    FlushableChannel putLong(long value) throws IOException;

    @Override
    FlushableChannel putFloat(float value) throws IOException;

    @Override
    FlushableChannel putDouble(double value) throws IOException;

    @Override
    default FlushableChannel put(byte[] value, int length) throws IOException {
        return put(value, 0, length);
    }

    @Override
    FlushableChannel put(byte[] value, int offset, int length) throws IOException;

    @Override
    FlushableChannel putVersion(byte version) throws IOException;
}
