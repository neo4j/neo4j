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
package org.neo4j.kernel.api.database.enrichment;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.enrichment.WriteEnrichmentChannel;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.ValueType;

@ExtendWith(RandomExtension.class)
class ValuesReadWriteTest {

    @Inject
    private RandomSupport random;

    @ParameterizedTest
    @EnumSource(ValueType.class)
    void roundTrips(ValueType type) throws IOException {
        final var value = random.randomValues().nextValueOfType(type);
        final var positions = new int[random.nextInt(50, 666)];
        try (var writeChannel = new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE)) {
            final var writer = new ValuesWriter(writeChannel);
            for (var i = 0; i < positions.length; i++) {
                positions[i] = writer.write(value);
            }

            var buffer = BufferBackedChannel.fill(writeChannel.flip());
            for (var i = 0; i < positions.length; i++) {
                final var reader = ValuesReader.BY_ID.get(buffer.get());
                assertThat(reader.read(buffer)).isEqualTo(value);
            }

            for (var position : positions) {
                final var reader =
                        ValuesReader.BY_ID.get(buffer.position(position).get());
                assertThat(reader.read(buffer)).isEqualTo(value);
            }
        }
    }

    private record BufferBackedChannel(ByteBuffer buffer) implements WritableChannel {

        static ByteBuffer fill(WriteEnrichmentChannel channel) throws IOException {
            final var buffer = ByteBuffer.allocate(channel.size()).order(ByteOrder.LITTLE_ENDIAN);
            try (var bufferChannel = new BufferBackedChannel(buffer)) {
                channel.serialize(bufferChannel);
                return buffer.flip();
            }
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
            buffer.put(value, offset, length);
            return this;
        }

        @Override
        public WritableChannel putAll(ByteBuffer src) {
            buffer.put(src);
            return this;
        }

        @Override
        public int write(ByteBuffer src) {
            final var remaining = src.remaining();
            buffer.put(src);
            return remaining;
        }

        @Override
        public void beginChecksumForWriting() {
            // no-op
        }

        @Override
        public int putChecksum() {
            // no-op
            return 0;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void close() {}
    }
}
