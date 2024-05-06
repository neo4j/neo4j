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
package org.neo4j.packstream.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.mockito.Mockito;
import org.neo4j.packstream.io.function.Writer;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.packstream.struct.StructWriter;

public class PackstreamBufWriteTest {

    private Stream<TypeMarker> getTypeMarkers() {
        return Stream.of(TypeMarker.values());
    }

    private ByteBuf prepareBuffer(Consumer<PackstreamBuf> provider) {
        var buffer = Unpooled.buffer();
        provider.accept(PackstreamBuf.wrap(buffer));
        return buffer;
    }

    @Test
    void shouldAllocate() {
        var alloc = mock(ByteBufAllocator.class);
        var buffer = mock(ByteBuf.class);

        when(alloc.buffer()).thenReturn(buffer);

        var wrapped = PackstreamBuf.alloc(alloc);

        assertThat(wrapped.getTarget()).isSameAs(buffer);

        verify(alloc).buffer();
        verifyNoMoreInteractions(alloc);
    }

    @Test
    void allocShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> PackstreamBuf.alloc(null));

        assertThat(ex).hasMessage("alloc cannot be null");
    }

    @Test
    void shouldWriteMarkerByte() {
        var buf = prepareBuffer(b -> b.writeMarkerByte(0x42));

        var actual = buf.readUnsignedByte();

        assertThat(actual).isEqualTo((short) 0x42);
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteMarker() {
        return getTypeMarkers()
                .filter(marker -> !marker.hasLengthPrefix())
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> b.writeMarker(marker));

                    var actual = buf.readUnsignedByte();

                    assertThat(actual).isEqualTo(marker.getValue());
                }));
    }

    @TestFactory
    Stream<DynamicTest> writeMarkerShouldFailWithIllegalArgumentExceptionWhenLengthPrefixIsOmitted() {
        return getTypeMarkers()
                .filter(TypeMarker::hasLengthPrefix)
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var ex = assertThrows(
                            IllegalArgumentException.class, () -> prepareBuffer(b -> b.writeMarker(marker)));

                    assertThat(ex).hasMessage("Type %s requires a length", marker.name());
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteMarkerWithLength() {
        return getTypeMarkers()
                .filter(TypeMarker::hasLengthPrefix)
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var expectedLength = marker.getLengthPrefix().getMaxValue();

                    var buf = prepareBuffer(b -> b.writeMarker(marker, expectedLength));

                    var actualMarker = buf.readUnsignedByte();

                    long actualLength;
                    switch (marker.getLengthPrefix()) {
                        case NIBBLE -> {
                            actualLength = actualMarker & 0x0F;
                            actualMarker &= 0xF0;
                        }
                        case UINT8 -> actualLength = buf.readUnsignedByte();
                        case UINT16 -> actualLength = buf.readUnsignedShort();
                        case UINT32 -> actualLength = buf.readUnsignedInt();
                        default -> throw new AssertionError(
                                "Invalid length prefix type " + marker.getType() + " for marker " + marker);
                    }

                    assertThat(actualMarker).isEqualTo(marker.getValue());
                    assertThat(actualLength).isEqualTo(expectedLength);
                }));
    }

    @TestFactory
    Stream<DynamicTest> writeMarkerShouldFailWithIllegalArgumentWhenLengthPrefixIsNotSupported() {
        return getTypeMarkers()
                .filter(marker -> !marker.hasLengthPrefix())
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var ex = assertThrows(
                            IllegalArgumentException.class, () -> prepareBuffer(b -> b.writeMarker(marker, 15)));

                    assertThat(ex).hasMessage("Type %s does not provide length", marker.name());
                }));
    }

    @TestFactory
    Stream<DynamicTest> writeMarkerShouldFailWithIllegalArgumentWhenLengthPrefixExceedsMaximum() {
        return getTypeMarkers()
                .filter(TypeMarker::hasLengthPrefix)
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var length = marker.getLengthPrefix().getMaxValue() + 1;

                    var ex = assertThrows(
                            IllegalArgumentException.class, () -> prepareBuffer(b -> b.writeMarker(marker, length)));

                    assertThat(ex)
                            .hasMessage(
                                    "Type %s cannot store value of length %d (limit is %d)",
                                    marker.name(),
                                    length,
                                    marker.getLengthPrefix().getMaxValue());
                }));
    }

    @TestFactory
    Stream<DynamicTest> writeMarkerShouldChooseSmallestPossiblePrefix() {
        return TypeMarker.STRING_TYPES.stream()
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> b.writeMarker(
                            TypeMarker.STRING_TYPES, marker.getLengthPrefix().getMaxValue()));

                    var actual = buf.readUnsignedByte();

                    if (marker.isNibbleMarker()) {
                        assertThat(actual).isEqualTo((short) (marker.getValue() ^ 15));
                    } else {
                        assertThat(actual).isEqualTo(marker.getValue());
                    }
                }));
    }

    @Test
    void writeMarkerShouldFailWithIllegalArgumentWhenNoMarkersAreGiven() {
        var ex = assertThrows(
                IllegalArgumentException.class, () -> prepareBuffer(b -> b.writeMarker(Collections.emptyList(), 42)));

        assertThat(ex).hasMessage("Marker collection cannot be empty");
    }

    @Test
    void writeMarkerShouldFailWithIllegalArgumentWhenLengthPrefixExceedsMaximumOfAllAlternatives() {
        var length = LengthPrefix.UINT32.getMaxValue() + 1;

        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> prepareBuffer(b -> b.writeMarker(TypeMarker.STRING_TYPES, length)));

        var maxLengths = TypeMarker.STRING_TYPES.stream()
                .map(marker -> String.format("%d (%s)", marker.getLengthPrefix().getMaxValue(), marker.name()))
                .collect(Collectors.joining(", "));

        assertThat(ex).hasMessage("Length %d exceeds supported maximum lengths of %s", length, maxLengths);
    }

    @Test
    void shouldWriteNull() {
        var buf = prepareBuffer(PackstreamBuf::writeNull);

        var actual = buf.readUnsignedByte();

        assertThat(actual).isEqualTo(TypeMarker.NULL.getValue());
        assertThat(buf.isReadable()).isFalse();
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteBoolean() {
        return Stream.of(Boolean.FALSE, Boolean.TRUE)
                .map(value -> dynamicTest(value.toString(), () -> {
                    var buf = prepareBuffer(b -> b.writeBoolean(value));

                    var actual = buf.readUnsignedByte();

                    assertThat(actual).isEqualTo((value ? TypeMarker.TRUE : TypeMarker.FALSE).getValue());
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt() {
        return Stream.of(
                        new MarkerExpectation<>(Long.MIN_VALUE, TypeMarker.INT64),
                        new MarkerExpectation<>(Integer.MIN_VALUE - 1L, TypeMarker.INT64),
                        new MarkerExpectation<>((long) Integer.MIN_VALUE, TypeMarker.INT32),
                        new MarkerExpectation<>(Short.MIN_VALUE - 1L, TypeMarker.INT32),
                        new MarkerExpectation<>((long) Short.MIN_VALUE, TypeMarker.INT16),
                        new MarkerExpectation<>(Byte.MIN_VALUE - 1L, TypeMarker.INT16),
                        new MarkerExpectation<>((long) Byte.MIN_VALUE, TypeMarker.INT8),
                        new MarkerExpectation<>(-17L, TypeMarker.INT8),
                        new MarkerExpectation<>(-16L, TypeMarker.TINY_INT),
                        new MarkerExpectation<>(-1L, TypeMarker.TINY_INT),
                        new MarkerExpectation<>(0L, TypeMarker.TINY_INT),
                        new MarkerExpectation<>(15L, TypeMarker.TINY_INT),
                        new MarkerExpectation<>(16L, TypeMarker.TINY_INT),
                        new MarkerExpectation<>((long) Byte.MAX_VALUE, TypeMarker.TINY_INT),
                        new MarkerExpectation<>(Byte.MAX_VALUE + 1L, TypeMarker.INT16),
                        new MarkerExpectation<>((long) Short.MAX_VALUE, TypeMarker.INT16),
                        new MarkerExpectation<>(Short.MAX_VALUE + 1L, TypeMarker.INT32),
                        new MarkerExpectation<>((long) Integer.MAX_VALUE, TypeMarker.INT32),
                        new MarkerExpectation<>(Integer.MAX_VALUE + 1L, TypeMarker.INT64),
                        new MarkerExpectation<>(Long.MAX_VALUE, TypeMarker.INT64))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    var buf = prepareBuffer(b -> b.writeInt(expectation.input()));

                    var marker = buf.readUnsignedByte();

                    long value;
                    if (expectation.marker() != TypeMarker.TINY_INT) {
                        value = switch (expectation.marker()) {
                            case INT8 -> buf.readByte();
                            case INT16 -> buf.readShort();
                            case INT32 -> buf.readInt();
                            case INT64 -> buf.readLong();
                            default -> throw new IllegalArgumentException("Invalid expectation: " + expectation);
                        };

                        assertThat(marker).isEqualTo(expectation.marker().getValue());
                    } else {
                        value = (byte) marker;
                    }

                    assertThat(value).isEqualTo(expectation.input());
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteTinyInt() {
        return IntStream.rangeClosed((int) Type.TINY_INT_MIN, (int) Type.TINY_INT_MAX)
                .mapToObj(value -> dynamicTest(Integer.toString(value), () -> {
                    var buf = prepareBuffer(b -> b.writeTinyInt((byte) value));

                    var actual = buf.readByte();

                    assertThat(actual).isEqualTo((byte) value);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeTinyIntShouldFailWithIllegalArgumentWhenValueOutOfRange() {
        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> prepareBuffer(b -> b.writeTinyInt((byte) (Type.TINY_INT_MIN - 1))));

        assertThat(ex).hasMessage("Value is out of type bounds: %d", Type.TINY_INT_MIN - 1);
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt8() {
        return IntStream.of((int) Type.INT8_MIN, 0, (int) Type.INT8_MAX)
                .mapToObj(value -> dynamicTest(Integer.toString(value), () -> {
                    var buf = prepareBuffer(b -> b.writeInt8((byte) value));

                    var marker = buf.readUnsignedByte();
                    var actualValue = buf.readByte();

                    assertThat(marker).isEqualTo(TypeMarker.INT8.getValue());
                    assertThat(actualValue).isEqualTo((byte) value);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt16() {
        return IntStream.of((int) Type.INT16_MIN, 0, (int) Type.INT16_MAX)
                .mapToObj(value -> dynamicTest(Integer.toString(value), () -> {
                    var buf = prepareBuffer(b -> b.writeInt16((short) value));

                    var marker = buf.readUnsignedByte();
                    var actualValue = buf.readShort();

                    assertThat(marker).isEqualTo(TypeMarker.INT16.getValue());
                    assertThat(actualValue).isEqualTo((short) value);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt32() {
        return IntStream.of((int) Type.INT32_MIN, 0, (int) Type.INT32_MAX)
                .mapToObj(value -> dynamicTest(Integer.toString(value), () -> {
                    var buf = prepareBuffer(b -> b.writeInt32(value));

                    var marker = buf.readUnsignedByte();
                    var actualValue = buf.readInt();

                    assertThat(marker).isEqualTo(TypeMarker.INT32.getValue());
                    assertThat(actualValue).isEqualTo(value);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteInt64() {
        return LongStream.of(Type.INT64_MIN, 0, Type.INT64_MAX)
                .mapToObj(value -> dynamicTest(Long.toString(value), () -> {
                    var buf = prepareBuffer(b -> b.writeInt64(value));

                    var marker = buf.readUnsignedByte();
                    var actualValue = buf.readLong();

                    assertThat(marker).isEqualTo(TypeMarker.INT64.getValue());
                    assertThat(actualValue).isEqualTo(value);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteFloat() {
        return DoubleStream.of(-0.125, -0.25, -0.5, 0, 0.5, 0.25, 0.125)
                .mapToObj(value -> dynamicTest(String.format("%.2f", value), () -> {
                    var buf = prepareBuffer(b -> b.writeFloat(value));

                    var marker = buf.readUnsignedByte();
                    var actualValue = buf.readDouble();

                    assertThat(marker).isEqualTo(TypeMarker.FLOAT64.getValue());
                    assertThat(actualValue).isEqualTo(value);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteBytes() {
        return Stream.of(
                        new MarkerExpectation<>(0L, TypeMarker.BYTES8),
                        new MarkerExpectation<>(1L, TypeMarker.BYTES8),
                        new MarkerExpectation<>(LengthPrefix.UINT8.getMaxValue(), TypeMarker.BYTES8),
                        new MarkerExpectation<>(LengthPrefix.UINT8.getMaxValue() + 1, TypeMarker.BYTES16),
                        new MarkerExpectation<>(LengthPrefix.UINT16.getMaxValue(), TypeMarker.BYTES16),
                        new MarkerExpectation<>(LengthPrefix.UINT16.getMaxValue() + 1, TypeMarker.BYTES32))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    var payload = new byte[(int) (long) expectation.input()];
                    for (var i = 0; i < payload.length; i++) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeBytes(Unpooled.wrappedBuffer(payload)));

                    var marker = buf.readUnsignedByte();
                    var length =
                            switch (expectation.marker()) {
                                case BYTES8 -> buf.readUnsignedByte();
                                case BYTES16 -> buf.readUnsignedShort();
                                case BYTES32 -> buf.readInt();
                                default -> throw new IllegalArgumentException("Invalid expectation: " + expectation);
                            };

                    var actualPayload = new byte[payload.length];
                    buf.readBytes(actualPayload);

                    assertThat(marker).isEqualTo(expectation.marker().getValue());
                    assertThat(length).isEqualTo(payload.length);
                    assertThat(actualPayload).isEqualTo(payload);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void shouldWriteBytes8() {
        var payload = new byte[32];
        for (var i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        var buf = prepareBuffer(b -> b.writeBytes8(Unpooled.wrappedBuffer(payload)));

        var marker = buf.readUnsignedByte();
        var length = buf.readUnsignedByte();

        var actualPayload = new byte[payload.length];
        buf.readBytes(actualPayload);

        assertThat(marker).isEqualTo(TypeMarker.BYTES8.getValue());
        assertThat(length).isEqualTo((short) payload.length);
        assertThat(payload).isEqualTo(actualPayload);

        assertThat(buf.isReadable()).isFalse();
    }

    @Test
    void writeBytes8ShouldFailWithIllegalArgumentWhenPayloadExceedsLimit() {
        var limit = LengthPrefix.UINT8.getMaxValue();
        var length = limit + 1;

        var payload = new byte[(int) length];

        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> prepareBuffer(b -> b.writeBytes8(Unpooled.wrappedBuffer(payload))));

        assertThat(ex).hasMessage("Type BYTES8 cannot store value of length %d (limit is %d)", length, limit);
    }

    @Test
    void shouldWriteBytes16() {
        var payload = new byte[32];
        for (var i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        var buf = prepareBuffer(b -> b.writeBytes16(Unpooled.wrappedBuffer(payload)));

        var marker = buf.readUnsignedByte();
        var length = buf.readUnsignedShort();

        var actualPayload = new byte[payload.length];
        buf.readBytes(actualPayload);

        assertThat(marker).isEqualTo(TypeMarker.BYTES16.getValue());
        assertThat(length).isEqualTo(payload.length);
        assertThat(payload).isEqualTo(actualPayload);

        assertThat(buf.isReadable()).isFalse();
    }

    @Test
    void writeBytes16ShouldFailWithIllegalArgumentWhenPayloadExceedsLimit() {
        var limit = LengthPrefix.UINT16.getMaxValue();
        var length = limit + 1;

        var payload = new byte[(int) length];

        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> prepareBuffer(b -> b.writeBytes16(Unpooled.wrappedBuffer(payload))));

        assertThat(ex).hasMessage("Type BYTES16 cannot store value of length %d (limit is %d)", length, limit);
    }

    @Test
    void shouldWriteBytes32() {
        var payload = new byte[32];
        for (var i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }

        var buf = prepareBuffer(b -> b.writeBytes32(Unpooled.wrappedBuffer(payload)));

        var marker = buf.readUnsignedByte();
        var length = buf.readInt();

        var actualPayload = new byte[payload.length];
        buf.readBytes(actualPayload);

        assertThat(marker).isEqualTo(TypeMarker.BYTES32.getValue());
        assertThat(length).isEqualTo(payload.length);
        assertThat(payload).isEqualTo(actualPayload);

        assertThat(buf.isReadable()).isFalse();
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteString() {
        return Stream.of(
                        new MarkerExpectation<>(0, TypeMarker.TINY_STRING),
                        new MarkerExpectation<>(1, TypeMarker.TINY_STRING),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue(), TypeMarker.TINY_STRING),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue() + 1, TypeMarker.STRING8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue(), TypeMarker.STRING8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue() + 1, TypeMarker.STRING16),
                        new MarkerExpectation<>((int) LengthPrefix.UINT16.getMaxValue(), TypeMarker.STRING16),
                        new MarkerExpectation<>((int) LengthPrefix.UINT16.getMaxValue() + 1, TypeMarker.STRING32))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    var payload = "a".repeat(expectation.input);

                    var buf = prepareBuffer(b -> b.writeString(payload));

                    var marker = buf.readUnsignedByte();
                    var length =
                            switch (expectation.marker()) {
                                case TINY_STRING -> {
                                    var mb = marker;
                                    marker &= 0xF0;

                                    yield mb & 0x0F;
                                }
                                case STRING8 -> buf.readUnsignedByte();
                                case STRING16 -> buf.readUnsignedShort();
                                case STRING32 -> buf.readInt();
                                default -> throw new IllegalArgumentException("Invalid expectation: " + expectation);
                            };

                    var heap = new byte[expectation.input];
                    buf.readBytes(heap);

                    var actualPayload = new String(heap, Type.STRING_CHARSET);

                    assertThat(marker).isEqualTo(expectation.marker().getValue());
                    assertThat(length).isEqualTo((long) expectation.input);
                    assertThat(actualPayload).isEqualTo(payload);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeStringShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeString(null)));

        assertThat(ex).hasMessage("payload cannot be null");
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteTinyString() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeString(payload));

                    var marker = buf.readUnsignedByte();
                    var length = marker & 0x0F;
                    marker = (short) (marker & 0xF0);

                    var heap = new byte[size];
                    buf.readBytes(heap);

                    var actualPayload = new String(heap, Type.STRING_CHARSET);

                    assertThat(marker).isEqualTo(TypeMarker.TINY_STRING.getValue());
                    assertThat(length).isEqualTo(size);
                    assertThat(actualPayload).isEqualTo(payload);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeTinyStringShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeTinyString(null)));

        assertThat(ex).hasMessage("payload cannot be null");
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteString8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeString8(payload));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readUnsignedByte();

                    var heap = new byte[size];
                    buf.readBytes(heap);

                    var actualPayload = new String(heap, Type.STRING_CHARSET);

                    assertThat(marker).isEqualTo(TypeMarker.STRING8.getValue());
                    assertThat(length).isEqualTo((short) size);
                    assertThat(actualPayload).isEqualTo(payload);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeString8ShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeString8(null)));

        assertThat(ex).hasMessage("payload cannot be null");
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteString16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeString16(payload));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readUnsignedShort();

                    var heap = new byte[size];
                    buf.readBytes(heap);

                    var actualPayload = new String(heap, Type.STRING_CHARSET);

                    assertThat(marker).isEqualTo(TypeMarker.STRING16.getValue());
                    assertThat(length).isEqualTo(size);
                    assertThat(actualPayload).isEqualTo(payload);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeString16ShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeString16(null)));

        assertThat(ex).hasMessage("payload cannot be null");
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteString32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeString32(payload));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readInt();

                    var heap = new byte[size];
                    buf.readBytes(heap);

                    var actualPayload = new String(heap, Type.STRING_CHARSET);

                    assertThat(marker).isEqualTo(TypeMarker.STRING32.getValue());
                    assertThat(length).isEqualTo(size);
                    assertThat(actualPayload).isEqualTo(payload);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeString32ShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeString32(null)));

        assertThat(ex).hasMessage("payload cannot be null");
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteListHeader() {
        return Stream.of(
                        new MarkerExpectation<>(0, TypeMarker.TINY_LIST),
                        new MarkerExpectation<>(1, TypeMarker.TINY_LIST),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue(), TypeMarker.TINY_LIST),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue() + 1, TypeMarker.LIST8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue(), TypeMarker.LIST8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue() + 1, TypeMarker.LIST16),
                        new MarkerExpectation<>((int) LengthPrefix.UINT16.getMaxValue(), TypeMarker.LIST16),
                        new MarkerExpectation<>((int) LengthPrefix.UINT16.getMaxValue() + 1, TypeMarker.LIST32))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    var buf = prepareBuffer(b -> b.writeListHeader(expectation.input()));

                    var marker = buf.readUnsignedByte();
                    var length =
                            switch (expectation.marker()) {
                                case TINY_LIST -> {
                                    var mb = marker;
                                    marker &= 0xF0;

                                    yield mb & 0x0F;
                                }
                                case LIST8 -> buf.readUnsignedByte();
                                case LIST16 -> buf.readUnsignedShort();
                                case LIST32 -> buf.readInt();
                                default -> throw new IllegalArgumentException("Invalid expectation: " + expectation);
                            };

                    assertThat(marker).isEqualTo(expectation.marker().getValue());
                    assertThat(length).isEqualTo((long) expectation.input());

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeListHeaderShouldFailWithIllegalArgumentWhenNegativeSizeIsGiven() {
        var ex = assertThrows(IllegalArgumentException.class, () -> prepareBuffer(b -> b.writeListHeader(-1)));

        assertThat(ex).hasMessage("size cannot be negative");
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteList() {
        return Stream.of(
                        new MarkerExpectation<>(0, TypeMarker.TINY_LIST),
                        new MarkerExpectation<>(1, TypeMarker.TINY_LIST),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue(), TypeMarker.TINY_LIST),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue() + 1, TypeMarker.LIST8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue(), TypeMarker.LIST8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue() + 1, TypeMarker.LIST16),
                        new MarkerExpectation<>((int) LengthPrefix.UINT16.getMaxValue(), TypeMarker.LIST16),
                        new MarkerExpectation<>((int) LengthPrefix.UINT16.getMaxValue() + 1, TypeMarker.LIST32))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    {
                        var value = (Integer) 42;

                        var payload = Collections.nCopies(expectation.input, value);
                        var writer = Mockito.mock(Writer.class);

                        @SuppressWarnings("unchecked")
                        var buf = prepareBuffer(b -> b.writeList(payload, writer));

                        var marker = buf.readUnsignedByte();
                        var length =
                                switch (expectation.marker()) {
                                    case TINY_LIST -> {
                                        var mb = marker;
                                        marker &= 0xF0;

                                        yield mb & 0x0F;
                                    }
                                    case LIST8 -> buf.readUnsignedByte();
                                    case LIST16 -> buf.readUnsignedShort();
                                    case LIST32 -> buf.readInt();
                                    default -> throw new IllegalArgumentException(
                                            "Invalid expectation: " + expectation);
                                };

                        assertThat(marker).isEqualTo(expectation.marker().getValue());
                        assertThat(length).isEqualTo((long) expectation.input());

                        verify(writer, times(expectation.input)).write(notNull(), same(value));

                        assertThat(buf.isReadable()).isFalse();
                    }
                }));
    }

    @Test
    void writeListShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeList(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteTinyList() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var value = (Integer) 42;

                    var payload = Collections.nCopies(size, value);
                    var writer = Mockito.mock(Writer.class);

                    var buf = prepareBuffer(b -> b.writeTinyList(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = marker & 0x0F;
                    marker &= 0xF0;

                    assertThat(marker).isEqualTo(TypeMarker.TINY_LIST.getValue());
                    assertThat(length).isEqualTo(size);

                    verify(writer, times(size)).write(notNull(), same(value));

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeTinyListShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeTinyList(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteList8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var value = (Integer) 42;

                    var payload = Collections.nCopies(size, value);
                    var writer = Mockito.mock(Writer.class);

                    var buf = prepareBuffer(b -> b.writeList8(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readUnsignedByte();

                    assertThat(marker).isEqualTo(TypeMarker.LIST8.getValue());
                    assertThat(length).isEqualTo((short) size);

                    verify(writer, times(size)).write(notNull(), same(value));

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeList8ShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeList8(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteList16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var value = (Integer) 42;

                    var payload = Collections.nCopies(size, value);
                    var writer = Mockito.mock(Writer.class);

                    var buf = prepareBuffer(b -> b.writeList16(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readUnsignedShort();

                    assertThat(marker).isEqualTo(TypeMarker.LIST16.getValue());
                    assertThat(length).isEqualTo(size);

                    verify(writer, times(size)).write(notNull(), same(value));

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeList16ShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeList16(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteList32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var value = (Integer) 42;

                    var payload = Collections.nCopies(size, value);
                    var writer = Mockito.mock(Writer.class);

                    var buf = prepareBuffer(b -> b.writeList32(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = (int) buf.readUnsignedInt();

                    assertThat(marker).isEqualTo(TypeMarker.LIST32.getValue());
                    assertThat(length).isEqualTo(size);

                    verify(writer, times(size)).write(notNull(), same(value));

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeList32ShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeList32(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteMapHeader() {
        return Stream.of(
                        new MarkerExpectation<>(0L, TypeMarker.TINY_MAP),
                        new MarkerExpectation<>(1L, TypeMarker.TINY_MAP),
                        new MarkerExpectation<>(LengthPrefix.NIBBLE.getMaxValue(), TypeMarker.TINY_MAP),
                        new MarkerExpectation<>(LengthPrefix.NIBBLE.getMaxValue() + 1, TypeMarker.MAP8),
                        new MarkerExpectation<>(LengthPrefix.UINT8.getMaxValue(), TypeMarker.MAP8),
                        new MarkerExpectation<>(LengthPrefix.UINT8.getMaxValue() + 1, TypeMarker.MAP16),
                        new MarkerExpectation<>(LengthPrefix.UINT16.getMaxValue(), TypeMarker.MAP16),
                        new MarkerExpectation<>(LengthPrefix.UINT16.getMaxValue() + 1, TypeMarker.MAP32),
                        new MarkerExpectation<>((long) Integer.MAX_VALUE, TypeMarker.MAP32))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    var buf = prepareBuffer(b -> b.writeMapHeader(expectation.input()));

                    var marker = buf.readUnsignedByte();
                    var length =
                            switch (expectation.marker()) {
                                case TINY_MAP -> {
                                    var mb = marker;
                                    marker &= 0xF0;

                                    yield mb & 0x0F;
                                }
                                case MAP8 -> buf.readUnsignedByte();
                                case MAP16 -> buf.readUnsignedShort();
                                case MAP32 -> buf.readUnsignedInt();
                                default -> throw new IllegalArgumentException("Invalid expectation: " + expectation);
                            };

                    assertThat(marker).isEqualTo(expectation.marker().getValue());
                    assertThat(length).isEqualTo(expectation.input());

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeMapHeaderShouldFailWithIllegalArgumentWhenPayloadExceedsValidBounds() {
        var ex = assertThrows(
                IllegalArgumentException.class,
                () -> prepareBuffer(b -> b.writeMapHeader(((long) Integer.MAX_VALUE) + 1)));

        assertThat(ex).hasMessage("length exceeds limit of %d", Integer.MAX_VALUE);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteMap() {
        // Note: Tests for the maximum of MAP16 and minimum of MAP32 are omitted as they would generate too much data
        // thus
        // causing long builds and high memory consumption
        return Stream.of(
                        new MarkerExpectation<>(0, TypeMarker.TINY_MAP),
                        new MarkerExpectation<>(1, TypeMarker.TINY_MAP),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue(), TypeMarker.TINY_MAP),
                        new MarkerExpectation<>((int) LengthPrefix.NIBBLE.getMaxValue() + 1, TypeMarker.MAP8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue(), TypeMarker.MAP8),
                        new MarkerExpectation<>((int) LengthPrefix.UINT8.getMaxValue() + 1, TypeMarker.MAP16))
                .map(expectation -> dynamicTest(expectation.toString(), () -> {
                    var writer = mock(Writer.class);
                    var payload = IntStream.rangeClosed(1, expectation.input())
                            .mapToObj(i -> Map.entry(String.format("el_%d", i), i))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    var buf = prepareBuffer(b -> b.writeMap(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length =
                            switch (expectation.marker()) {
                                case TINY_MAP -> {
                                    var mb = marker;
                                    marker &= 0xF0;

                                    yield mb & 0x0F;
                                }
                                case MAP8 -> buf.readUnsignedByte();
                                case MAP16 -> buf.readUnsignedShort();
                                case MAP32 -> buf.readInt();
                                default -> throw new IllegalArgumentException("Invalid expectation: " + expectation);
                            };

                    assertThat(marker).isEqualTo(expectation.marker().getValue());
                    assertThat(length).isEqualTo(expectation.input());

                    var keys = new HashSet<String>();
                    for (var i = 1; i <= expectation.input(); i++) {
                        var keyMarker = buf.readUnsignedByte();
                        var keyLength = keyMarker & 0x0F;
                        keyMarker &= 0xF0;

                        assertThat(keyMarker).isEqualTo(TypeMarker.TINY_STRING.getValue());

                        var heap = new byte[keyLength];
                        buf.readBytes(heap);

                        var key = new String(heap, Type.STRING_CHARSET);

                        assertThat(keys.add(key)).isTrue();

                        verify(writer).write(notNull(), eq((Integer) i));
                    }
                    verifyNoMoreInteractions(writer);

                    assertThat(keys).hasSize(expectation.input());
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeMapShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeMap(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteTinyMap() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var writer = mock(Writer.class);
                    var payload = IntStream.rangeClosed(1, size)
                            .mapToObj(i -> Map.entry(String.format("el_%d", i), i))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    var buf = prepareBuffer(b -> b.writeTinyMap(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = marker & 0x0F;
                    marker &= 0xF0;

                    assertThat(marker).isEqualTo(TypeMarker.TINY_MAP.getValue());
                    assertThat(length).isEqualTo(size);

                    var keys = new HashSet<String>();
                    for (var i = 1; i <= size; i++) {
                        var keyMarker = buf.readUnsignedByte();
                        var keyLength = keyMarker & 0x0F;
                        keyMarker &= 0xF0;

                        assertThat(keyMarker).isEqualTo(TypeMarker.TINY_STRING.getValue());

                        var heap = new byte[keyLength];
                        buf.readBytes(heap);

                        var key = new String(heap, Type.STRING_CHARSET);

                        assertThat(keys.add(key)).isTrue();

                        verify(writer).write(notNull(), eq((Integer) i));
                    }
                    verifyNoMoreInteractions(writer);

                    assertThat(keys).hasSize(size);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeTinyMapShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeTinyMap(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteMap8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var writer = mock(Writer.class);
                    var payload = IntStream.rangeClosed(1, size)
                            .mapToObj(i -> Map.entry(String.format("el_%d", i), i))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    var buf = prepareBuffer(b -> b.writeMap8(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readUnsignedByte();

                    assertThat(marker).isEqualTo(TypeMarker.MAP8.getValue());
                    assertThat(length).isEqualTo((short) size);

                    var keys = new HashSet<String>();
                    for (var i = 1; i <= size; i++) {
                        var keyMarker = buf.readUnsignedByte();
                        var keyLength = keyMarker & 0x0F;
                        keyMarker &= 0xF0;

                        assertThat(keyMarker).isEqualTo(TypeMarker.TINY_STRING.getValue());

                        var heap = new byte[keyLength];
                        buf.readBytes(heap);

                        var key = new String(heap, Type.STRING_CHARSET);

                        assertThat(keys.add(key)).isTrue();

                        verify(writer).write(notNull(), eq((Integer) i));
                    }
                    verifyNoMoreInteractions(writer);

                    assertThat(keys).hasSize(size);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeMap8ShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeMap8(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteMap16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var writer = mock(Writer.class);
                    var payload = IntStream.rangeClosed(1, size)
                            .mapToObj(i -> Map.entry(String.format("el_%d", i), i))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    var buf = prepareBuffer(b -> b.writeMap16(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = buf.readUnsignedShort();

                    assertThat(marker).isEqualTo(TypeMarker.MAP16.getValue());
                    assertThat(length).isEqualTo(size);

                    var keys = new HashSet<String>();
                    for (var i = 1; i <= size; i++) {
                        var keyMarker = buf.readUnsignedByte();
                        var keyLength = keyMarker & 0x0F;
                        keyMarker &= 0xF0;

                        assertThat(keyMarker).isEqualTo(TypeMarker.TINY_STRING.getValue());

                        var heap = new byte[keyLength];
                        buf.readBytes(heap);

                        var key = new String(heap, Type.STRING_CHARSET);

                        assertThat(keys.add(key)).isTrue();

                        verify(writer).write(notNull(), eq((Integer) i));
                    }
                    verifyNoMoreInteractions(writer);

                    assertThat(keys).hasSize(size);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeMap16ShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeMap16(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteMap32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var writer = mock(Writer.class);
                    var payload = IntStream.rangeClosed(1, size)
                            .mapToObj(i -> Map.entry(String.format("el_%d", i), i))
                            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

                    var buf = prepareBuffer(b -> b.writeMap32(payload, writer));

                    var marker = buf.readUnsignedByte();
                    var length = (int) buf.readUnsignedInt();

                    assertThat(marker).isEqualTo(TypeMarker.MAP32.getValue());
                    assertThat(length).isEqualTo(size);

                    var keys = new HashSet<String>();
                    for (var i = 1; i <= size; i++) {
                        var keyMarker = buf.readUnsignedByte();
                        var keyLength = keyMarker & 0x0F;
                        keyMarker &= 0xF0;

                        assertThat(keyMarker).isEqualTo(TypeMarker.TINY_STRING.getValue());

                        var heap = new byte[keyLength];
                        buf.readBytes(heap);

                        var key = new String(heap, Type.STRING_CHARSET);

                        assertThat(keys.add(key)).isTrue();

                        verify(writer).write(notNull(), eq((Integer) i));
                    }
                    verifyNoMoreInteractions(writer);

                    assertThat(keys).hasSize(size);
                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @Test
    void writeMap32ShouldFailWithNullPointerWhenNullIsGiven() {
        var writer = mock(Writer.class);

        @SuppressWarnings("unchecked")
        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeMap32(null, writer)));

        assertThat(ex).hasMessage("payload cannot be null");

        verifyNoInteractions(writer);
    }

    @TestFactory
    Stream<DynamicTest> shouldWriteStructHeader() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(fields -> dynamicTest(String.format("%d fields", fields), () -> {
                    var buf = prepareBuffer(b -> b.writeStructHeader(new StructHeader(fields, (short) 0x42)));

                    var marker = buf.readUnsignedByte();
                    var length = marker & 0x0F;
                    marker &= 0xF0;
                    var tag = buf.readUnsignedByte();

                    assertThat(marker).isEqualTo(TypeMarker.TINY_STRUCT.getValue());
                    assertThat(length).isEqualTo((short) fields);
                    assertThat(tag).isEqualTo((short) 0x42);

                    assertThat(buf.isReadable()).isFalse();
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldWriteStruct() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(fields -> dynamicTest(String.format("%d fields", fields), () -> {
                    var payload = new Object();
                    var writer = mock(StructWriter.class);
                    var registry = mock(StructRegistry.class);
                    var ctx = mock(Object.class);

                    when(registry.getWriter(payload)).thenReturn(Optional.<StructWriter<Object, Object>>of(writer));
                    when(writer.getTag(payload)).thenReturn((short) 0x21);
                    when(writer.getLength(payload)).thenReturn((long) fields);

                    var buf = prepareBuffer(b -> b.writeStruct(ctx, registry, payload));

                    var marker = buf.readUnsignedByte();
                    var length = marker & 0x0F;
                    marker &= 0xF0;
                    var tag = buf.readUnsignedByte();

                    assertThat(marker).isEqualTo(TypeMarker.TINY_STRUCT.getValue());
                    assertThat(length).isEqualTo(fields);
                    assertThat(tag).isEqualTo((short) 0x21);

                    var inOrder = inOrder(registry, writer);

                    inOrder.verify(registry).getWriter(payload);

                    inOrder.verify(writer).getTag(payload);
                    inOrder.verify(writer).getLength(payload);
                    inOrder.verify(writer).write(same(ctx), notNull(), same(payload));

                    inOrder.verifyNoMoreInteractions();
                }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void writeStructShouldFailWithIllegalArgumentWhenRegistryReturnsEmptyOptional() {
        var payload = new Object();
        var registry = mock(StructRegistry.class);

        when(registry.getWriter(payload)).thenReturn(Optional.empty());

        var ex = assertThrows(
                IllegalArgumentException.class, () -> prepareBuffer(b -> b.writeStruct(null, registry, payload)));

        assertThat(ex).hasMessage("Illegal struct: %s", payload);

        verify(registry).getWriter(payload);
        verifyNoMoreInteractions(registry);
    }

    @Test
    void writeStructShouldFailWithNullPointerWhenRegistryIsNull() {
        var payload = new Object();

        var ex = assertThrows(NullPointerException.class, () -> prepareBuffer(b -> b.writeStruct(null, null, payload)));

        assertThat(ex).hasMessage("registry cannot be null");
    }

    @Test
    @SuppressWarnings("unchecked")
    void writeStructShouldAcceptNullPayloads() {
        var registry = mock(StructRegistry.class);
        var writer = mock(StructWriter.class);

        when(registry.getWriter(null)).thenReturn(Optional.<StructWriter<Object, Object>>of(writer));
        when(writer.getTag(null)).thenReturn((short) 0x09);
        when(writer.getLength(null)).thenReturn(4L);

        var buf = prepareBuffer(b -> b.writeStruct(null, registry, null));

        var marker = buf.readUnsignedByte();
        var length = marker & 0x0F;
        marker &= 0xF0;
        var tag = buf.readUnsignedByte();

        assertThat(marker).isEqualTo(TypeMarker.TINY_STRUCT.getValue());
        assertThat(length).isEqualTo(4);
        assertThat(tag).isEqualTo((short) 0x09);
    }

    private record MarkerExpectation<I>(I input, TypeMarker marker) {

        @Override
        public String toString() {
            return this.marker + ": " + this.input;
        }
    }
}
