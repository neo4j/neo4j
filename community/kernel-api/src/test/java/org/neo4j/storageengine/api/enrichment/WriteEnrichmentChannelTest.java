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
package org.neo4j.storageengine.api.enrichment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.neo4j.storageengine.api.enrichment.WriteEnrichmentChannel.CHUNK_SIZE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.IntStream;
import org.eclipse.collections.api.factory.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.neo4j.io.fs.BufferBackedChannel;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;

@ExtendWith(RandomExtension.class)
class WriteEnrichmentChannelTest {

    @Inject
    private RandomSupport random;

    @Test
    void memoryTracking() {
        // given
        final var allocated = ArgumentCaptor.forClass(Long.class);
        final var released = ArgumentCaptor.forClass(Long.class);
        final var tracker = mock(MemoryTracker.class);
        doNothing().when(tracker).allocateHeap(allocated.capture());
        doNothing().when(tracker).releaseHeap(released.capture());

        final var labels = IntStream.range(0, CHUNK_SIZE / Integer.BYTES).toArray();

        final long memoryUsed;
        try (var channel = new WriteEnrichmentChannel(tracker)) {
            assertThat(sum(allocated))
                    .as("should have allocated the chunks list")
                    .isGreaterThan(0L);

            channel.putLong(42L);

            // then
            assertThat(sum(allocated))
                    .as("should have allocated a chunk to the buffer")
                    .isGreaterThan(CHUNK_SIZE)
                    .isLessThan(CHUNK_SIZE * 2);

            // when
            for (var label : labels) {
                channel.putInt(label);
            }

            // then
            memoryUsed = sum(allocated);
            assertThat(memoryUsed)
                    .as("should have allocated a new chunk to the buffer")
                    .isGreaterThan(CHUNK_SIZE * 2)
                    .isLessThan(CHUNK_SIZE * 3);
        }

        assertThat(sum(released)).as("should have released all the memory used").isEqualTo(memoryUsed);
    }

    @Test
    void position() {
        final var bytes = random.nextBytes(new byte[CHUNK_SIZE]);

        try (var channel = channel()) {
            assertThat(channel.size()).isEqualTo(0L);

            channel.putLong(42L);
            assertThat(channel.size()).isEqualTo(Long.BYTES);

            channel.putLong(43L);
            assertThat(channel.size()).isEqualTo(2 * Long.BYTES);

            channel.put(bytes);
            assertThat(channel.size()).isEqualTo(CHUNK_SIZE + (2 * Long.BYTES));

            channel.put(bytes);
            channel.putLong(44L);
            assertThat(channel.size()).isEqualTo((CHUNK_SIZE * 2) + (3 * Long.BYTES));
        }
    }

    @Test
    void putAll() {
        final var bytesSize = (CHUNK_SIZE * 2) + random.nextInt(1, CHUNK_SIZE);
        final var bytes = random.nextBytes(new byte[bytesSize]);
        final var buffer = ByteBuffer.wrap(bytes);

        try (var channel = channel()) {
            channel.put(bytes);
            assertThat(channel.size()).isEqualTo(bytesSize);

            channel.putAll(buffer);
            assertThat(channel.size()).isEqualTo(bytesSize * 2);
        }
    }

    @Test
    void positionCrossingChunks() {
        final var bytesSize = CHUNK_SIZE - 2;
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            assertThat(channel.size()).isEqualTo(0L);

            channel.put(bytes);
            assertThat(channel.size()).isEqualTo(bytesSize);

            channel.putLong(44L);
            assertThat(channel.size()).isEqualTo(bytesSize + Long.BYTES);
        }
    }

    @Test
    void peek() {
        final var dataAndIndex = dataAndIndex(random, Byte.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            assertThat(channel.peek(dataAndIndex.ix)).isEqualTo(bytes[dataAndIndex.ix]);
            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peek(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekChar() {
        final var dataAndIndex = dataAndIndex(random, Character.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            assertThat(channel.peekChar(dataAndIndex.ix))
                    .isEqualTo(fillForPrimitive(Character.BYTES, bytes, dataAndIndex.ix)
                            .getChar());
            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peekChar(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekShort() {
        final var dataAndIndex = dataAndIndex(random, Short.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            assertThat(channel.peekShort(dataAndIndex.ix))
                    .isEqualTo(fillForPrimitive(Short.BYTES, bytes, dataAndIndex.ix)
                            .getShort());
            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peekShort(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekInt() {
        final var dataAndIndex = dataAndIndex(random, Integer.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            assertThat(channel.peekInt(dataAndIndex.ix))
                    .isEqualTo(fillForPrimitive(Integer.BYTES, bytes, dataAndIndex.ix)
                            .getInt());
            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peekInt(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekLong() {
        final var dataAndIndex = dataAndIndex(random, Long.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            assertThat(channel.peekLong(dataAndIndex.ix))
                    .isEqualTo(
                            fillForPrimitive(Long.BYTES, bytes, dataAndIndex.ix).getLong());
            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peekLong(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekFloat() {
        final var dataAndIndex = dataAndIndex(random, Float.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            final var actual = channel.peekFloat(dataAndIndex.ix);
            final var expected =
                    fillForPrimitive(Float.BYTES, bytes, dataAndIndex.ix).getFloat();
            if (Float.isNaN(expected)) {
                assertThat(actual).isNaN();
            } else {
                assertThat(actual).isEqualTo(expected);
            }

            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peekFloat(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekDouble() {
        final var dataAndIndex = dataAndIndex(random, Double.BYTES);
        final var bytes = dataAndIndex.bytes(random);

        try (var channel = channel()) {
            channel.put(bytes);

            final var actual = channel.peekDouble(dataAndIndex.ix);
            final var expected =
                    fillForPrimitive(Double.BYTES, bytes, dataAndIndex.ix).getDouble();
            if (Double.isNaN(expected)) {
                assertThat(actual).isNaN();
            } else {
                assertThat(actual).isEqualTo(expected);
            }

            assertThat(channel.size()).as("position should not have changed").isEqualTo(dataAndIndex.dataSize);

            assertThatThrownBy(() -> channel.peekDouble(dataAndIndex.dataSize))
                    .as("peeking outside of the channel's contents is an error")
                    .isInstanceOf(BufferOverflowException.class);
        }
    }

    @Test
    void peekCharCrossingChunks() {
        final var value = (char) 42;
        final var bytesSize = CHUNK_SIZE - 1;
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            channel.put(bytes);
            channel.putChar(value);
            assertThat(channel.peekChar(bytesSize)).isEqualTo(value);
        }
    }

    @Test
    void peekShortCrossingChunks() {
        final var value = (short) 42;
        final var bytesSize = CHUNK_SIZE - 1;
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            channel.put(bytes);
            channel.putShort(value);
            assertThat(channel.peekShort(bytesSize)).isEqualTo(value);
        }
    }

    @Test
    void peekIntCrossingChunks() {
        final var value = 42;
        final var bytesSize = CHUNK_SIZE - random.nextInt(1, Integer.BYTES - 1);
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            channel.put(bytes);
            channel.putInt(value);
            assertThat(channel.peekInt(bytesSize)).isEqualTo(value);
        }
    }

    @Test
    void peekLongCrossingChunks() {
        final var value = 42L;
        final var bytesSize = CHUNK_SIZE - random.nextInt(1, Long.BYTES - 1);
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            channel.put(bytes);
            channel.putLong(value);
            assertThat(channel.peekLong(bytesSize)).isEqualTo(value);
        }
    }

    @Test
    void peekFloatCrossingChunks() {
        final var value = 42.0f;
        final var bytesSize = CHUNK_SIZE - random.nextInt(1, Float.BYTES - 1);
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            channel.put(bytes);
            channel.putFloat(value);
            assertThat(channel.peekFloat(bytesSize)).isEqualTo(value);
        }
    }

    @Test
    void peekDoubleCrossingChunks() {
        final var value = 42.0;
        final var bytesSize = CHUNK_SIZE - random.nextInt(1, Double.BYTES - 1);
        final var bytes = random.nextBytes(new byte[bytesSize]);

        try (var channel = channel()) {
            channel.put(bytes);
            channel.putDouble(value);
            assertThat(channel.peekDouble(bytesSize)).isEqualTo(value);
        }
    }

    @Test
    void serializeRequiresFlippedChannel() {
        try (var channel = channel();
                var buffer = new BufferBackedChannel(channel.size())) {
            assertThatThrownBy(() -> channel.serialize(buffer))
                    .isInstanceOf(IOException.class)
                    .hasMessage("Please ensure that the channel has been flipped");
        }
    }

    @Test
    void serialize() throws IOException {
        final var lines = Lists.mutable.<PrimitiveLine>empty();
        for (int i = 0, length = random.nextInt(1, 5000); i < length; i++) {
            lines.add(PrimitiveLine.create(random));
        }

        try (var channel = channel()) {
            for (var line : lines) {
                channel.put(line.b)
                        .putChar(line.c)
                        .putShort(line.s)
                        .putInt(line.i)
                        .putLong(line.l)
                        .putFloat(line.f)
                        .putDouble(line.d);
            }

            final var sizeBeforeFlip = channel.size();
            try (var buffer = new BufferBackedChannel(sizeBeforeFlip)) {
                final var flipped = channel.flip();
                assertThat(flipped.size())
                        .as("size should remain the same after a flip")
                        .isEqualTo(sizeBeforeFlip);
                flipped.serialize(buffer);
                assertThat(flipped.size())
                        .as("size should remain the same after a serialize")
                        .isEqualTo(sizeBeforeFlip);
                buffer.flip();

                PrimitiveLine currentLine = null;
                var readType = PrimitiveRead.BYTE;
                while (!lines.isEmpty()) {
                    switch (readType) {
                        case BYTE -> {
                            currentLine = lines.remove(0);
                            assertThat(currentLine.b).isEqualTo(buffer.get());
                        }
                        case CHAR -> assertThat(currentLine.c).isEqualTo(buffer.getChar());
                        case SHORT -> assertThat(currentLine.s).isEqualTo(buffer.getShort());
                        case INT -> assertThat(currentLine.i).isEqualTo(buffer.getInt());
                        case LONG -> assertThat(currentLine.l).isEqualTo(buffer.getLong());
                        case FLOAT -> assertThat(currentLine.f).isEqualTo(buffer.getFloat());
                        case DOUBLE -> assertThat(currentLine.d).isEqualTo(buffer.getDouble());
                    }

                    readType = readType.next();
                }

                assertThat(lines.isEmpty()).as("should have read all the data").isTrue();
            }
        }
    }

    @Test
    void serializeShouldBeAbleToRunConcurrently() {
        final var count = 13;
        try (var channel = channel()) {
            for (var i = 0L; i < count; i++) {
                channel.putLong(i);
            }

            channel.flip();

            final var race = new Race();
            race.addContestants(
                    count,
                    () -> {
                        try (var buffer = new BufferBackedChannel(channel.size())) {
                            channel.serialize(buffer);
                            buffer.flip();

                            for (var i = 0L; i < count; i++) {
                                assertThat(buffer.getLong()).isEqualTo(i);
                            }
                        } catch (IOException ex) {
                            throw new UncheckedIOException(ex);
                        }
                    },
                    666);
            race.goUnchecked();
        }
    }

    private static WriteEnrichmentChannel channel() {
        return new WriteEnrichmentChannel(EmptyMemoryTracker.INSTANCE);
    }

    private static ByteBuffer fillForPrimitive(int primitiveSize, byte[] bytes, int atPosition) {
        return ByteBuffer.allocate(primitiveSize)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put(bytes, atPosition, primitiveSize)
                .flip();
    }

    private static long sum(ArgumentCaptor<Long> captor) {
        return captor.getAllValues().stream().mapToLong(l -> l).sum();
    }

    private static DataAndIndex dataAndIndex(RandomSupport random, int byteDataSize) {
        final var dataSize = (CHUNK_SIZE * 2) + random.nextInt(1, CHUNK_SIZE);
        final var slotCount = dataSize / byteDataSize;
        final var ix = random.nextInt(0, slotCount - 1) * byteDataSize;
        return new DataAndIndex(dataSize, ix);
    }

    private record DataAndIndex(int dataSize, int ix) {
        byte[] bytes(RandomSupport random) {
            return random.nextBytes(new byte[dataSize]);
        }
    }

    private record PrimitiveLine(byte b, char c, short s, int i, long l, float f, double d) {
        private static PrimitiveLine create(RandomSupport random) {
            return new PrimitiveLine(
                    (byte) random.nextInt(),
                    (char) random.nextInt(),
                    (short) random.nextInt(),
                    random.nextInt(),
                    random.nextLong(),
                    random.nextFloat(),
                    random.nextDouble());
        }
    }

    private enum PrimitiveRead {
        BYTE,
        CHAR,
        SHORT,
        INT,
        LONG,
        FLOAT,
        DOUBLE;

        private static final List<PrimitiveRead> VALUES = List.of(values());

        private PrimitiveRead next() {
            return VALUES.get((ordinal() + 1) % VALUES.size());
        }
    }
}
