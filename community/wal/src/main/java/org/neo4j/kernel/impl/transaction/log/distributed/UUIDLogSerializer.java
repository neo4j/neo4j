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
package org.neo4j.kernel.impl.transaction.log.distributed;

import java.io.IOException;
import java.util.UUID;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

/**
 * Helper methods for (de)serializing UUID elements into the log files
 */
public class UUIDLogSerializer {
    private static final byte NO_UUID = (byte) 0;
    private static final byte UUID_PRESENT = (byte) 1;

    /** Number of bytes written by {@link #write(WritableChannel, UUID)}. */
    public static final int UUID_BYTES = Long.BYTES + Long.BYTES;

    private UUIDLogSerializer() {}

    /**
     * Write UUID into a channel as two longs
     * @param channel the target channel to write into
     * @param uuid a non-NULL UUID instance
     * @throws IOException thrown by errors on the underlying channel
     */
    public static void write(WritableChannel channel, UUID uuid) throws IOException {
        channel.putLong(uuid.getMostSignificantBits());
        channel.putLong(uuid.getLeastSignificantBits());
    }

    /**
     * Reads back a UUID written previously by write
     * @param channel the source channel
     * @return the parsed UUID
     * @throws IOException thrown by errors on the underlying channel
     */
    public static UUID parse(ReadableChannel channel) throws IOException {
        long mostSigBits = channel.getLong();
        long leastSigBits = channel.getLong();
        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Writes a marker byte to indicate optional presence/absence of UUID
     * followed by two longs for the UUID if present
     * @param channel the target channel to write into
     * @param uuid a UUID instance, or null if absent
     * @throws IOException thrown by errors on the underlying channel
     */
    public static void writeNullable(WritableChannel channel, UUID uuid) throws IOException {
        if (uuid == null) {
            channel.put(NO_UUID);
        } else {
            channel.put(UUID_PRESENT);
            write(channel, uuid);
        }
    }

    /**
     * @return the number of bytes written by {@link #writeNullable(WritableChannel, UUID)} for the given UUID.
     */
    public static int nullableByteSize(UUID uuid) {
        return uuid == null ? Byte.BYTES : Byte.BYTES + UUID_BYTES;
    }

    /**
     * Reads back an optional UUID written previously by writeNullable
     * @param channel the source channel
     * @return either null, or the parsed UUID
     * @throws IOException thrown by errors on the underlying channel
     */
    public static UUID parseNullable(ReadableChannel channel) throws IOException {
        byte nullMarker = channel.get();
        if (nullMarker == NO_UUID) {
            return null;
        }
        return parse(channel);
    }
}
