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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.fs.WritableChannel;

class StoreIdTest {
    private final String ENGINE_1 = "storage-engine-1";
    private final String FORMAT_FAMILY_1 = "format-family-1";
    private final String ENGINE_2 = "storage-engine-2";
    private final String FORMAT_FAMILY_2 = "format-family-2";

    @Test
    void testCompatibilityCheck() {
        var storeId = new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7);
        assertTrue(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7)));
        assertTrue(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 8)));
        assertTrue(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 15)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(666, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 666, ENGINE_1, FORMAT_FAMILY_1, 3, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 6)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 4, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 2, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_2, FORMAT_FAMILY_1, 3, 7)));
        assertFalse(storeId.isSameOrUpgradeSuccessor(new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_2, 3, 7)));
    }

    @Test
    void testSerialization() throws IOException {
        var buffer = new ChannelBuffer(100);
        var storeId = new StoreId(1234, 789, ENGINE_1, FORMAT_FAMILY_1, 3, 7);
        storeId.serialize(buffer);
        buffer.flip();
        var deserializedStoreId = StoreId.deserialize(buffer);
        assertEquals(storeId, deserializedStoreId);
    }

    private static class ChannelBuffer implements WritableChannel, ReadableChannel {

        private final ByteBuffer buffer;
        private boolean isClosed;

        ChannelBuffer(int capacity) {
            this.buffer = ByteBuffer.allocate(capacity);
        }

        @Override
        public byte get() {
            return buffer.get();
        }

        @Override
        public short getShort() {
            return buffer.getShort();
        }

        @Override
        public int getInt() {
            return buffer.getInt();
        }

        @Override
        public long getLong() {
            return buffer.getLong();
        }

        @Override
        public float getFloat() {
            return buffer.getFloat();
        }

        @Override
        public double getDouble() {
            return buffer.getDouble();
        }

        @Override
        public void get(byte[] bytes, int length) {
            buffer.get(bytes, 0, length);
        }

        @Override
        public WritableChannel put(byte value) {
            buffer.put(value);
            return this;
        }

        @Override
        public WritableChannel putShort(short value) {
            buffer.putShort(value);
            return this;
        }

        @Override
        public WritableChannel putInt(int value) {
            buffer.putInt(value);
            return this;
        }

        @Override
        public WritableChannel putLong(long value) {
            buffer.putLong(value);
            return this;
        }

        @Override
        public WritableChannel putFloat(float value) {
            buffer.putFloat(value);
            return this;
        }

        @Override
        public WritableChannel putDouble(double value) {
            buffer.putDouble(value);
            return this;
        }

        @Override
        public WritableChannel put(byte[] value, int offset, int length) {
            buffer.put(value, 0, length);
            return this;
        }

        @Override
        public WritableChannel putAll(ByteBuffer src) {
            buffer.put(src);
            return null;
        }

        @Override
        public boolean isOpen() {
            return !isClosed;
        }

        @Override
        public void close() {
            isClosed = true;
        }

        void flip() {
            buffer.flip();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            int remaining = dst.remaining();
            dst.put(buffer);
            return remaining;
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            int remaining = src.remaining();
            buffer.put(src);
            return remaining;
        }

        @Override
        public int getChecksum() {
            return BASE_TX_CHECKSUM;
        }

        @Override
        public int endChecksumAndValidate() {
            return buffer.getInt();
        }

        @Override
        public void beginChecksum() {}

        @Override
        public int putChecksum() {
            buffer.putInt(BASE_TX_CHECKSUM);
            return BASE_TX_CHECKSUM;
        }

        @Override
        public long position() throws IOException {
            return buffer.position();
        }

        @Override
        public void position(long byteOffset) throws IOException {
            buffer.position(Math.toIntExact(byteOffset));
        }
    }
}
