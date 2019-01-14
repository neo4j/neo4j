/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.storageengine.api;

import java.io.Closeable;
import java.io.IOException;

/**
 * Represents a channel from where primitive values can be read. Mirrors {@link WritableChannel} in
 * data types that can be read.
 */
public interface ReadableChannel extends Closeable
{
    /**
     * @return the next {@code byte} in this channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    byte get() throws IOException;

    /**
     * @return the next {@code short} in this channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    short getShort() throws IOException;

    /**
     * @return the next {@code int} in this channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    int getInt() throws IOException;

    /**
     * @return the next {@code long} in this channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    long getLong() throws IOException;

    /**
     * @return the next {@code float} in this channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    float getFloat() throws IOException;

    /**
     * @return the next {@code double} in this channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    double getDouble() throws IOException;

    /**
     * Reads the next {@code length} bytes from this channel and puts them into {@code bytes}.
     * Will throw {@link ArrayIndexOutOfBoundsException} if {@code length} exceeds the length of {@code bytes}.
     *
     * @param bytes {@code byte[]} to put read bytes into.
     * @param length number of bytes to read from the channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     */
    void get( byte[] bytes, int length ) throws IOException;
}
