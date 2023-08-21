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
package org.neo4j.storageengine.api;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.util.Preconditions;

public class StoreIdSerialization {

    /**
     * Serialised StoreId is used in places that require fixed size length.
     * This constant is used as the limit in those places.
     * The constant is present here mainly as a reminder
     * of the limit for future modifiers of the serialisation logic.
     */
    public static final int MAX_STORE_ID_LENGTH = 64;

    /**
     * There is currently only one version of the serialisation format and hopefully it will stay like that.
     * The version existence is there only for future-proofness.
     * Naturally, if we ever have more than one version, the serialisation and deserialisation code must be modified accordingly.
     */
    private static final byte VERSION = 0x01;

    static void serialize(StoreId storeId, WritableChannel channel) throws IOException {
        serialize(storeId, new Writer() {
            @Override
            public void writeByte(byte value) throws IOException {
                channel.put(value);
            }

            @Override
            public void writeLong(long value) throws IOException {
                channel.putLong(value);
            }

            @Override
            public void writeByteArray(byte[] value, int length) throws IOException {
                channel.put(value, length);
            }
        });
    }

    static StoreId deserialize(ReadableChannel channel) throws IOException {
        return deserialize(new Reader() {

            @Override
            public byte readByte() throws IOException {
                return channel.get();
            }

            @Override
            public long readLong() throws IOException {
                return channel.getLong();
            }

            @Override
            public void readByteArray(byte[] value, int length) throws IOException {
                channel.get(value, length);
            }
        });
    }

    /**
     * Serialises the submitted store ID into the submitted buffer using exactly {@link #MAX_STORE_ID_LENGTH} bytes.
     * This method is for Kernel internal use only as the value of {@link #MAX_STORE_ID_LENGTH} can change
     * and thus making possible external users very sad.
     */
    public static void serializeWithFixedSize(StoreId storeId, ByteBuffer buffer) throws IOException {
        if (buffer.remaining() < MAX_STORE_ID_LENGTH) {
            throw new IllegalArgumentException("The submitted buffer is too small");
        }

        int originalLimit = buffer.limit();
        // let's set limit, so the write methods blow up if the ID does not fit to the dedicated fixed-size slot
        buffer.limit(buffer.position() + MAX_STORE_ID_LENGTH);
        serialize(storeId, new Writer() {

            @Override
            public void writeByte(byte value) {
                buffer.put(value);
            }

            @Override
            public void writeLong(long value) {
                buffer.putLong(value);
            }

            @Override
            public void writeByteArray(byte[] value, int length) {
                buffer.put(value, 0, length);
            }
        });

        while (buffer.hasRemaining()) {
            buffer.put((byte) 0);
        }

        buffer.limit(originalLimit);
    }

    /**
     * Deserialises a store ID from the submitted buffer using exactly {@link #MAX_STORE_ID_LENGTH} bytes from the buffer.
     * Like its pair method {@link #serializeWithFixedSize}, this method is for Kernel internal use only.
     */
    public static StoreId deserializeWithFixedSize(ByteBuffer buffer) throws IOException {
        if (buffer.remaining() < MAX_STORE_ID_LENGTH) {
            throw new IllegalArgumentException("The submitted buffer is too small");
        }

        int originalLimit = buffer.limit();
        // let's set limit, so to make 100% sure, we don't read beyond the dedicated fixed-size slot
        buffer.limit(buffer.position() + MAX_STORE_ID_LENGTH);

        var storeId = deserialize(new Reader() {

            @Override
            public byte readByte() {
                return buffer.get();
            }

            @Override
            public long readLong() {
                return buffer.getLong();
            }

            @Override
            public void readByteArray(byte[] value, int length) {
                buffer.get(value, 0, length);
            }
        });

        buffer.position(buffer.limit());
        buffer.limit(originalLimit);
        return storeId;
    }

    private static void serialize(StoreId storeId, Writer writer) throws IOException {
        byte[] storageEngine = storeId.getStorageEngineName().getBytes(StandardCharsets.UTF_8);
        byte[] formatFamily = storeId.getFormatName().getBytes(StandardCharsets.UTF_8);
        Preconditions.requireBetween(storageEngine.length, 0, 256);
        Preconditions.requireBetween(formatFamily.length, 0, 256);
        // The store id allows beta versions - represented by a negative major version
        Preconditions.requireBetween(storeId.getMajorVersion(), -128, 128);
        // For minor version we can do more restrictive checks
        // Unfortunately, there are some test formats using NO_GENERATION = -1
        Preconditions.requireBetween(storeId.getMinorVersion(), -1, 128);

        writer.writeByte(VERSION);
        writer.writeLong(storeId.getCreationTime());
        writer.writeLong(storeId.getRandom());
        writer.writeByte((byte) storageEngine.length);
        writer.writeByteArray(storageEngine, storageEngine.length);
        writer.writeByte((byte) formatFamily.length);
        writer.writeByteArray(formatFamily, formatFamily.length);
        writer.writeByte((byte) storeId.getMajorVersion());
        writer.writeByte((byte) storeId.getMinorVersion());
    }

    private static StoreId deserialize(Reader reader) throws IOException {
        byte version = reader.readByte();
        if (version != VERSION) {
            throw new IllegalArgumentException("Unknown serialization format version: " + version);
        }

        long creationTime = reader.readLong();
        long randomId = reader.readLong();
        String storageEngine = deserializeString(reader);
        String formatFamily = deserializeString(reader);
        int majorVersion = reader.readByte();
        int minorVersion = reader.readByte();
        return new StoreId(creationTime, randomId, storageEngine, formatFamily, majorVersion, minorVersion);
    }

    private static String deserializeString(Reader reader) throws IOException {
        int length = reader.readByte() & 0xFF;
        byte[] bytes = new byte[length];
        reader.readByteArray(bytes, length);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private interface Writer {
        void writeByte(byte value) throws IOException;

        void writeLong(long value) throws IOException;

        void writeByteArray(byte[] value, int length) throws IOException;
    }

    private interface Reader {
        byte readByte() throws IOException;

        long readLong() throws IOException;

        void readByteArray(byte[] value, int length) throws IOException;
    }
}
