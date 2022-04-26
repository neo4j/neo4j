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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.util.Preconditions;

class StoreIdSerialization {
    /**
     * There is currently only one version of the serialisation format and hopefully it will stay like that.
     * The version existence is there only for future-proofness.
     * Naturally, if we ever have more than one version, the serialisation and deserialisation code must be modified accordingly.
     */
    private static final byte VERSION = 0x01;

    static void serialize(StoreId storeId, WritableChannel channel) throws IOException {
        byte[] storageEngine = storeId.getStorageEngineName().getBytes(StandardCharsets.UTF_8);
        byte[] formatFamily = storeId.getFormatFamilyName().getBytes(StandardCharsets.UTF_8);
        Preconditions.requireBetween(storageEngine.length, 0, 256);
        Preconditions.requireBetween(formatFamily.length, 0, 256);
        Preconditions.requireBetween(storeId.getMajorVersion(), 0, 256);
        Preconditions.requireBetween(storeId.getMinorVersion(), 0, 256);

        channel.put(VERSION);
        channel.putLong(storeId.getCreationTime());
        channel.putLong(storeId.getRandom());
        channel.put((byte) storageEngine.length);
        channel.put(storageEngine, storageEngine.length);
        channel.put((byte) formatFamily.length);
        channel.put(formatFamily, formatFamily.length);
        channel.put((byte) storeId.getMajorVersion());
        channel.put((byte) storeId.getMinorVersion());
    }

    static StoreId deserialize(ReadableChannel channel) throws IOException {
        byte version = channel.get();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unknown serialization format version: " + version);
        }

        long creationTime = channel.getLong();
        long randomId = channel.getLong();
        String storageEngine = deserializeString(channel);
        String formatFamily = deserializeString(channel);
        int majorVersion = channel.get() & 0xFF;
        int minorVersion = channel.get() & 0xFF;
        return new StoreId(creationTime, randomId, storageEngine, formatFamily, majorVersion, minorVersion);
    }

    static String deserializeString(ReadableChannel channel) throws IOException {
        int length = channel.get() & 0xFF;
        byte[] bytes = new byte[length];
        channel.get(bytes, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
