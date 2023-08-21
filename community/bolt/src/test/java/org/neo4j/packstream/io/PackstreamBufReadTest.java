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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.notNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.function.ThrowingConsumer;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedStructException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.reader.UnexpectedTypeMarkerException;
import org.neo4j.packstream.io.function.Reader;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.packstream.struct.StructRegistry;

@Timeout(value = 5, unit = TimeUnit.MINUTES)
class PackstreamBufReadTest {

    private static PackstreamBuf prepareBuffer(Consumer<ByteBuf> supplier) {
        var buffer = Unpooled.buffer();
        supplier.accept(buffer);
        return PackstreamBuf.wrap(buffer);
    }

    private static Stream<Type> getValidTypes() {
        return Stream.of(Type.values()).filter(type -> type != Type.NONE);
    }

    private static Stream<TypeMarker> getValidMarkers() {
        return Stream.of(TypeMarker.values())
                .filter(marker -> marker != TypeMarker.RESERVED && marker != TypeMarker.TINY_INT);
    }

    private static Stream<TypeMarker> getValidMarkers(Type excluded) {
        return getValidMarkers().filter(marker -> marker.getType() != excluded);
    }

    private static Stream<TypeMarker> getValidMarkers(TypeMarker excluded) {
        return getValidMarkers().filter(marker -> marker != excluded);
    }

    private static IntStream getVariations(TypeMarker base) {
        if (base == TypeMarker.RESERVED) {
            return IntStream.empty();
        }

        if (base == TypeMarker.TINY_INT) {
            return IntStream.rangeClosed((int) Type.TINY_INT_MIN, (int) Type.TINY_INT_MAX);
        } else if (base.isNibbleMarker()) {
            return IntStream.rangeClosed(0x0, 0xF).map(variation -> base.getValue() ^ variation);
        } else {
            return IntStream.of(base.getValue());
        }
    }

    private static void assertThrowsUnexpectedType(
            Type expected, TypeMarker actual, ThrowingConsumer<PackstreamBuf> consumer) {
        getVariations(actual).forEach(variation -> {
            var buf = prepareBuffer(b -> b.writeByte(variation));

            var ex = assertThrows(UnexpectedTypeException.class, () -> consumer.accept(buf));

            assertThat(ex.getExpected()).isEqualTo(expected);
            assertThat(ex.getActual()).isEqualTo(actual.getType());
        });
    }

    private static void assertThrowsUnexpectedTypeMarker(
            TypeMarker expected, TypeMarker actual, ThrowingConsumer<PackstreamBuf> consumer) {
        getVariations(actual).forEach(variation -> {
            var buf = prepareBuffer(b -> b.writeByte(variation));

            var ex = assertThrows(UnexpectedTypeMarkerException.class, () -> consumer.accept(buf));

            assertThat(ex.getExpected()).isEqualTo(expected.getType());
            assertThat(ex.getActual()).isEqualTo(actual.getType());

            assertThat(ex.getExpectedMarker()).isEqualTo(expected);
            assertThat(ex.getActualMarker()).isEqualTo(actual);
        });
    }

    @Test
    void shouldCreateSimpleWrappedBuffers() {
        {
            var buffer = mock(ByteBuf.class);
            var wrapped = PackstreamBuf.wrap(buffer);

            assertThat(wrapped.getTarget()).isSameAs(buffer);
        }

        {
            var buffer = mock(ByteBuf.class);

            when(buffer.retain()).thenReturn(buffer);

            var wrapped = PackstreamBuf.wrapRetained(buffer);

            assertThat(wrapped.getTarget()).isSameAs(buffer);
        }
    }

    @Test
    void wrapShouldNotIncrementReferenceCount() {
        var buffer = mock(ByteBuf.class);
        PackstreamBuf.wrap(buffer);

        verify(buffer, never()).retain();
        verify(buffer, never()).retain(anyInt());
    }

    @Test
    void wrapRetainedShouldIncrementReferenceCount() {
        var buffer = mock(ByteBuf.class);

        when(buffer.retain()).thenReturn(buffer);

        PackstreamBuf.wrapRetained(buffer);

        verify(buffer).retain();
    }

    @Test
    void wrapShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> PackstreamBuf.wrap(null));

        assertThat(ex).hasMessage("delegate cannot be null");
    }

    @Test
    void wrapRetainedShouldFailWithNullPointerWhenNullIsGiven() {
        var ex = assertThrows(NullPointerException.class, () -> PackstreamBuf.wrapRetained(null));

        assertThat(ex).hasMessage("delegate cannot be null");
    }

    @TestFactory
    Stream<DynamicTest> shouldReadMarkerByte() {
        return Stream.of(TypeMarker.values())
                .filter(marker -> marker != TypeMarker.RESERVED)
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> b.writeByte(marker.getValue()));

                    var markerA = buf.readMarkerByte();

                    assertThat(markerA).isEqualTo(marker.getValue());

                    assertThat(buf.getTarget().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> testReadTypeMarker() {
        return Stream.of(TypeMarker.values())
                .filter(marker -> marker != TypeMarker.RESERVED)
                .map(expected -> dynamicTest(expected.name(), () -> {
                    var variations = getVariations(expected);

                    variations.forEach(variation -> {
                        var buf = prepareBuffer(b -> b.writeByte(variation));
                        var actual = buf.readMarker();

                        assertThat(actual).isSameAs(expected);

                        assertThat(buf.getTarget().isReadable()).isFalse();
                    });
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadExpectedTypeMarker() {
        return Stream.of(TypeMarker.values())
                .filter(marker -> marker != TypeMarker.RESERVED)
                .map(expected -> dynamicTest(
                        expected.name(), () -> getVariations(expected).forEach(valid -> {
                            var buf = prepareBuffer(b -> b.writeByte(valid));

                            try {
                                var mb = buf.readExpectedMarker(expected);

                                if (expected == TypeMarker.TINY_INT) {
                                    assertThat(mb).isEqualTo(Byte.toUnsignedLong((byte) valid));
                                } else if (expected.isNibbleMarker()) {
                                    assertThat(mb).isEqualTo(valid & 0x0F);
                                } else {
                                    assertThat(mb).isEqualTo(valid);
                                }

                                assertThat(buf.getTarget().isReadable()).isFalse();
                            } catch (UnexpectedTypeMarkerException ex) {
                                throw new AssertionError(String.format("Failed to decode variation 0x%02X", valid), ex);
                            }
                        })));
    }

    @TestFactory
    Stream<DynamicTest> readExpectedTypeMarkerShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers()
                .map(expected -> dynamicTest(expected.name(), () -> getValidMarkers(expected)
                        .forEach(invalid -> assertThrowsUnexpectedTypeMarker(
                                expected, invalid, buf -> buf.readExpectedMarker(expected)))));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadLengthPrefixMarker() {
        return Arrays.stream(TypeMarker.values())
                .filter(TypeMarker::hasLengthPrefix)
                .filter(marker -> !marker.isNibbleMarker())
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> {
                        b.writeByte(marker.getValue());
                        marker.getLengthPrefix().writeTo(b, 42);
                    });

                    var actual = buf.readLengthPrefixMarker(marker.getType(), -1);

                    assertThat(actual).isEqualTo(42);

                    assertThat(buf.getTarget().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> readLengthPrefixMarkerShouldFailWithUnexpectedType() {
        return getValidTypes()
                .map(expected -> dynamicTest(expected.name(), () -> getValidMarkers(expected)
                        .forEach(invalid -> assertThrowsUnexpectedType(
                                expected, invalid, buf -> buf.readLengthPrefixMarker(expected, -1)))));
    }

    @TestFactory
    Stream<DynamicTest> readLengthPrefixMarkerShouldFailWithLimitExceeded() {
        return Arrays.stream(TypeMarker.values())
                .filter(TypeMarker::hasLengthPrefix)
                .filter(marker -> !marker.isNibbleMarker())
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> {
                        b.writeByte(marker.getValue());
                        marker.getLengthPrefix().writeTo(b, 43);
                    });

                    var ex = assertThrows(
                            LimitExceededException.class, () -> buf.readLengthPrefixMarker(marker.getType(), 42));

                    assertThat(ex.getLimit()).isEqualTo(42);
                    assertThat(ex.getActual()).isEqualTo(43);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldPeekMarkerByte() {
        return Stream.of(TypeMarker.values())
                .filter(marker -> marker != TypeMarker.RESERVED)
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> b.writeByte(marker.getValue()));

                    var mb = buf.peekMarkerByte();

                    assertThat(mb).isEqualTo(marker.getValue());

                    assertThat(buf.getTarget().readableBytes()).isEqualTo(1);
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldPeekMarker() {
        return Stream.of(TypeMarker.values())
                .filter(marker -> marker != TypeMarker.RESERVED)
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> b.writeByte(marker.getValue()));

                    var mb = buf.peekMarker();

                    assertThat(mb).isEqualTo(marker);

                    assertThat(buf.getTarget().readableBytes()).isEqualTo(1);
                }));
    }

    @Test
    void shouldReadNull() throws UnexpectedTypeMarkerException {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.NULL.getValue()));

        assertSame(buf, buf.readNull());

        assertThat(buf.getTarget().isReadable()).isFalse();
    }

    @TestFactory
    Stream<DynamicTest> readNullShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.NULL)
                .map(marker -> dynamicTest(
                        marker.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.NULL, marker, PackstreamBuf::readNull)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadBoolean() {
        return TypeMarker.BOOLEAN_VALUES.stream()
                .map(marker -> dynamicTest(marker.name(), () -> {
                    try {
                        var buf = prepareBuffer(b -> b.writeByte(marker.getValue()));

                        var expected = marker == TypeMarker.TRUE;
                        var actual = buf.readBoolean();

                        assertThat(actual).isEqualTo(expected);
                    } catch (UnexpectedTypeException ex) {
                        throw new AssertionError(ex);
                    }
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBooleanShouldFailWithUnexpectedType() {
        return getValidMarkers(Type.BOOLEAN)
                .map(marker -> dynamicTest(
                        marker.name(),
                        () -> assertThrowsUnexpectedType(Type.BOOLEAN, marker, PackstreamBuf::readBoolean)));
    }

    @TestFactory
    Stream<DynamicTest> readIntShouldAcceptTinyInt() {
        return IntStream.rangeClosed((int) Type.TINY_INT_MIN, (int) Type.TINY_INT_MAX)
                .mapToObj(expected -> dynamicTest(String.format("0x%02X", (byte) expected), () -> {
                    var buf = prepareBuffer(b -> b.writeByte(expected));

                    var actual = buf.readInt();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readIntShouldAcceptInt8() {
        return IntStream.of((int) Type.INT8_MIN, (int) Type.TINY_INT_MIN - 1, (int) Type.INT8_MAX)
                .mapToObj(expected -> dynamicTest(Integer.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT8.getValue()).writeByte(expected));

                    var actual = buf.readInt();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readIntShouldAcceptInt16() {
        return IntStream.of((int) Type.INT16_MIN, (int) Type.INT16_MAX)
                .mapToObj(expected -> dynamicTest(Integer.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT16.getValue()).writeShort(expected));

                    var actual = buf.readInt();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readIntShouldAcceptInt32() {
        return IntStream.of((int) Type.INT32_MIN, (int) Type.INT32_MAX)
                .mapToObj(expected -> dynamicTest(Integer.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT32.getValue()).writeInt(expected));

                    var actual = buf.readInt();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readIntShouldAcceptInt64() {
        return LongStream.of(Type.INT64_MIN, Type.INT64_MAX)
                .mapToObj(expected -> dynamicTest(Long.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT64.getValue()).writeLong(expected));

                    var actual = buf.readInt();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readIntShouldFailWithUnexpectedType() {
        return getValidMarkers(Type.INT)
                .map(marker -> dynamicTest(
                        marker.name(), () -> assertThrowsUnexpectedType(Type.INT, marker, PackstreamBuf::readInt)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadTinyInt() {
        return IntStream.rangeClosed((int) Type.TINY_INT_MIN, (int) Type.TINY_INT_MAX)
                .mapToObj(expected -> dynamicTest(String.format("0x%02X", (byte) expected), () -> {
                    var buf = prepareBuffer(b -> b.writeByte(expected));

                    var actual = buf.readTinyInt();

                    assertThat(actual).isEqualTo((byte) expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readTinyIntShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers()
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.TINY_INT, invalid, PackstreamBuf::readTinyInt)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadInt8() {
        return IntStream.of((int) Type.INT8_MIN, (int) Type.INT8_MAX)
                .mapToObj(expected -> dynamicTest(Integer.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT8.getValue()).writeByte(expected));

                    var actual = buf.readInt8();

                    assertThat(actual).isEqualTo((byte) expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readInt8ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.INT8)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.INT8, invalid, PackstreamBuf::readInt8)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadInt16() {
        return IntStream.of((int) Type.INT16_MIN, (int) Type.INT16_MAX)
                .mapToObj(expected -> dynamicTest(Integer.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT16.getValue()).writeShort(expected));

                    var actual = buf.readInt16();

                    assertThat(actual).isEqualTo((short) expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readInt16ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.INT16)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.INT16, invalid, PackstreamBuf::readInt16)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadInt32() {
        return IntStream.of((int) Type.INT32_MIN, (int) Type.INT32_MAX)
                .mapToObj(expected -> dynamicTest(Integer.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT32.getValue()).writeInt(expected));

                    var actual = buf.readInt32();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readInt32ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.INT32)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.INT32, invalid, PackstreamBuf::readInt32)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadInt64() {
        return LongStream.of(Type.INT64_MIN, Type.INT64_MAX)
                .mapToObj(expected -> dynamicTest(Long.toString(expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.INT64.getValue()).writeLong(expected));

                    var actual = buf.readInt64();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readInt64ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.INT64)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.INT64, invalid, PackstreamBuf::readInt64)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadFloat() {
        return DoubleStream.of(0.125, 0.25, 0.5, 1, 2, 4, 8)
                .mapToObj(expected -> dynamicTest(String.format("%.2f", expected), () -> {
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.FLOAT64.getValue()).writeDouble(expected));

                    var actual = buf.readFloat();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readFloatShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.FLOAT64)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.FLOAT64, invalid, PackstreamBuf::readFloat)));
    }

    @TestFactory
    Stream<DynamicTest> readBytesShouldAcceptBytes8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d bytes", size), () -> {
                    var payload = new byte[size];
                    for (var i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES8.getValue())
                            .writeByte(size)
                            .writeBytes(payload));

                    var actual = buf.readBytes();

                    assertThat(actual).isEqualTo(Unpooled.wrappedBuffer(payload));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBytesShouldAcceptBytes16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d bytes", size), () -> {
                    var payload = new byte[size];
                    for (var i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES16.getValue())
                            .writeShort(size)
                            .writeBytes(payload));

                    var actual = buf.readBytes();

                    assertThat(actual).isEqualTo(Unpooled.wrappedBuffer(payload));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBytesShouldAcceptBytes32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d bytes", size), () -> {
                    var payload = new byte[size];
                    for (var i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES32.getValue())
                            .writeInt(size)
                            .writeBytes(payload));

                    var actual = buf.readBytes();

                    assertThat(actual).isEqualTo(Unpooled.wrappedBuffer(payload));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBytesShouldFailWithUnexpectedType() {
        return getValidMarkers(Type.BYTES)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedType(Type.BYTES, invalid, PackstreamBuf::readBytes)));
    }

    @TestFactory
    Stream<DynamicTest> readBytesShouldFailWithLimitExceeded() {
        return TypeMarker.BYTES_TYPES.stream()
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> {
                        b.writeByte(marker.getValue());
                        marker.getLengthPrefix().writeTo(b, 43);
                    });

                    var ex = assertThrows(LimitExceededException.class, () -> buf.readBytes(42));

                    assertThat(ex.getLimit()).isEqualTo(42);
                    assertThat(ex.getActual()).isEqualTo(43);
                }));
    }

    @Test
    void readBytesShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, buf::readBytes);

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);
    }

    @TestFactory
    Stream<DynamicTest> shouldReadBytes8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d bytes", size), () -> {
                    var payload = new byte[size];
                    for (var i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES8.getValue())
                            .writeByte(size)
                            .writeBytes(payload));

                    var actual = buf.readBytes8();

                    assertThat(actual).isEqualTo(Unpooled.wrappedBuffer(payload));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBytes8ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.BYTES8)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(TypeMarker.BYTES8, invalid, PackstreamBuf::readBytes8)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadBytes16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d bytes", size), () -> {
                    var payload = new byte[size];
                    for (var i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES16.getValue())
                            .writeShort(size)
                            .writeBytes(payload));

                    var actual = buf.readBytes16();

                    assertThat(actual).isEqualTo(Unpooled.wrappedBuffer(payload));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBytes16ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.BYTES16)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.BYTES16, invalid, PackstreamBuf::readBytes16)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadBytes32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d bytes", size), () -> {
                    var payload = new byte[size];
                    for (var i = 0; i < payload.length; ++i) {
                        payload[i] = (byte) i;
                    }

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES32.getValue())
                            .writeInt(size)
                            .writeBytes(payload));

                    var actual = buf.readBytes32();

                    assertThat(actual).isEqualTo(Unpooled.wrappedBuffer(payload));
                }));
    }

    @TestFactory
    Stream<DynamicTest> readBytes32ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.BYTES32)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.BYTES32, invalid, PackstreamBuf::readBytes32)));
    }

    @Test
    void shouldBytes32ShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.BYTES32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, buf::readBytes32);

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);
    }

    @TestFactory
    Stream<DynamicTest> readStringShouldAcceptTinyString() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.TINY_STRING.getValue() ^ size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readStringShouldAcceptString8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING8.getValue())
                            .writeByte(size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readStringShouldAcceptString16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING16.getValue())
                            .writeShort(size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readStringShouldAcceptString32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING32.getValue())
                            .writeInt(size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readStringShouldFailWithLimitExceeded() {
        return TypeMarker.STRING_TYPES.stream()
                .map(marker -> dynamicTest(marker.name(), () -> {
                    var buf = prepareBuffer(b -> {
                        if (marker.isNibbleMarker()) {
                            b.writeByte(marker.getValue() ^ 15);
                        } else {
                            b.writeByte(marker.getValue());
                            marker.getLengthPrefix().writeTo(b, 43);
                        }
                    });

                    var ex = assertThrows(LimitExceededException.class, () -> {
                        if (marker.isNibbleMarker()) {
                            buf.readString(14);
                        } else {
                            buf.readString(42);
                        }
                    });

                    if (marker.isNibbleMarker()) {
                        assertThat(ex.getLimit()).isEqualTo(14);
                        assertThat(ex.getActual()).isEqualTo(15);
                    } else {
                        assertThat(ex.getLimit()).isEqualTo(42);
                        assertThat(ex.getActual()).isEqualTo(43);
                    }
                }));
    }

    @Test
    void readStringShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, buf::readString);

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);
    }

    @TestFactory
    Stream<DynamicTest> shouldReadTinyString() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.TINY_STRING.getValue() ^ size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readTinyString();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readTinyStringShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.TINY_STRING)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.TINY_STRING, invalid, PackstreamBuf::readTinyString)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadString8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING8.getValue())
                            .writeByte(size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString8();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readString8ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.STRING8)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.STRING8, invalid, PackstreamBuf::readString8)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadString16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING16.getValue())
                            .writeShort(size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString16();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readString16ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.STRING16)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.STRING16, invalid, PackstreamBuf::readString16)));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadString32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d characters", size), () -> {
                    var payload = "a".repeat(size);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING32.getValue())
                            .writeInt(size)
                            .writeBytes(payload.getBytes(StandardCharsets.UTF_8)));

                    var actual = buf.readString32();

                    assertThat(actual).isEqualTo(payload);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readString32ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.STRING32)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.STRING32, invalid, PackstreamBuf::readString32)));
    }

    @Test
    void readString32ShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.STRING32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, buf::readString32);

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);
    }

    @TestFactory
    Stream<DynamicTest> readListShouldAcceptTinyList() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.TINY_LIST.getValue() ^ size));

                    @SuppressWarnings("unchecked")
                    var actual = buf.readList(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readListShouldAcceptList8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.LIST8.getValue()).writeByte(size));

                    @SuppressWarnings("unchecked")
                    var actual = buf.readList(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readListShouldAcceptList16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.LIST16.getValue()).writeShort(size));

                    @SuppressWarnings("unchecked")
                    var actual = buf.readList(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readListShouldAcceptList32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(
                            b -> b.writeByte(TypeMarker.LIST32.getValue()).writeInt(size));

                    @SuppressWarnings("unchecked")
                    var actual = buf.readList(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @Test
    @SuppressWarnings("unchecked")
    void readListShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var reader = mock(Reader.class);
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.LIST32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, () -> buf.readList(reader));

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);

        verifyNoMoreInteractions(reader);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMapShouldAcceptTinyMap() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.TINY_MAP.getValue() ^ size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMapShouldAcceptMap8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.MAP8.getValue()).writeByte(size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMapShouldAcceptMap16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.MAP16.getValue()).writeShort(size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMapShouldAcceptMap32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.MAP32.getValue()).writeInt(size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readMapShouldFailWithUnexpectedType() {
        return getValidMarkers(Type.MAP)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedType(
                                Type.MAP, invalid, buf -> buf.readMap(Mockito.mock(Reader.class)))));
    }

    @Test
    @SuppressWarnings("unchecked")
    void readMapShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.MAP32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, () -> buf.readMap(mock(Reader.class)));

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldReadTinyMap() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.TINY_MAP.getValue() ^ size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readTinyMap(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readTinyMapShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.TINY_MAP)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.TINY_MAP, invalid, buf -> buf.readTinyMap(mock(Reader.class)))));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldReadMap8() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT8.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.MAP8.getValue()).writeByte(size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap8(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMap8ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.MAP8)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.MAP8, invalid, buf -> buf.readMap8(mock(Reader.class)))));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldReadMap16() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.MAP16.getValue()).writeShort(size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap16(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMap16ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.MAP16)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.MAP16, invalid, buf -> buf.readMap16(mock(Reader.class)))));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldReadMap32() {
        return IntStream.of(0, 1, (int) LengthPrefix.UINT16.getMaxValue() + 1)
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var reader = mock(Reader.class);
                    var buf = prepareBuffer(b -> {
                        b.writeByte(TypeMarker.MAP32.getValue()).writeInt(size);

                        for (var i = 0; i < size; ++i) {
                            var key = Integer.toHexString(i).getBytes(StandardCharsets.UTF_8);

                            b.writeByte(TypeMarker.TINY_STRING.getValue() ^ key.length)
                                    .writeBytes(key);
                        }
                    });

                    var actual = buf.readMap32(reader);

                    assertThat(actual).hasSize(size);

                    verify(reader, times(size)).read(buf);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readMap32ShouldFailWithUnexpectedTypeMarker() {
        return getValidMarkers(TypeMarker.MAP32)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedTypeMarker(
                                TypeMarker.MAP32, invalid, buf -> buf.readMap32(mock(Reader.class)))));
    }

    @Test
    @SuppressWarnings("unchecked")
    void readMap32ShouldFailWithLimitExceededWhenUnsignedInt32IsGiven() {
        var buf = prepareBuffer(b -> b.writeByte(TypeMarker.MAP32.getValue()).writeInt(0xFFFFFFFF));

        var ex = assertThrows(LimitExceededException.class, () -> buf.readMap32(mock(Reader.class)));

        assertThat(ex.getLimit()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ex.getActual()).isEqualTo(0xFFFFFFFFL);
    }

    @TestFactory
    Stream<DynamicTest> shouldReadStructHeaders() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.TINY_STRUCT.getValue() ^ size)
                            .writeByte(0x42));

                    var actual = buf.readStructHeader();

                    assertThat(actual.length()).isEqualTo(size);
                    assertThat(actual.tag()).isEqualTo((short) 0x42);
                }));
    }

    @TestFactory
    Stream<DynamicTest> readStructHeaderShouldFailWithUnexpectedType() {
        return getValidMarkers(Type.STRUCT)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedType(Type.STRUCT, invalid, PackstreamBuf::readStructHeader)));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> shouldReadStruct() {
        return IntStream.rangeClosed(0, (int) LengthPrefix.NIBBLE.getMaxValue())
                .mapToObj(size -> dynamicTest(String.format("%d elements", size), () -> {
                    var registry = mock(StructRegistry.class);
                    var reader = mock(StructReader.class);
                    var headerCaptor = ArgumentCaptor.forClass(StructHeader.class);
                    var ctx = Mockito.mock(Object.class);

                    var buf = prepareBuffer(b -> b.writeByte(TypeMarker.TINY_STRUCT.getValue() ^ size)
                            .writeByte(0x42));

                    when(registry.getReader(headerCaptor.capture())).thenReturn(Optional.of(reader));

                    buf.readStruct(ctx, registry);

                    var header = headerCaptor.getValue();

                    assertThat(header.length()).isEqualTo(size);
                    assertThat(header.tag()).isEqualTo((short) 0x42);

                    verify(registry).getReader(header);
                    verifyNoMoreInteractions(registry);

                    verify(reader).read(ctx, buf, header);
                    verifyNoMoreInteractions(reader);
                }));
    }

    @TestFactory
    @SuppressWarnings("unchecked")
    Stream<DynamicTest> readStructShouldFailWithUnexpectedType() {
        return getValidMarkers(Type.STRUCT)
                .map(invalid -> dynamicTest(
                        invalid.name(),
                        () -> assertThrowsUnexpectedType(
                                Type.STRUCT, invalid, buf -> buf.readStruct(null, mock(StructRegistry.class)))));
    }

    @Test
    @SuppressWarnings("unchecked")
    void readStructShouldFailWithUnexpectedStruct() {
        var registry = mock(StructRegistry.class);
        var buf = prepareBuffer(
                b -> b.writeByte(TypeMarker.TINY_STRUCT.getValue()).writeByte(0x42));

        when(registry.getReader(notNull())).thenReturn(Optional.empty());

        var ex = assertThrows(UnexpectedStructException.class, () -> buf.readStruct(null, registry));

        assertThat(ex.getLength()).isEqualTo(0);
        assertThat(ex.getTag()).isEqualTo((short) 0x42);
    }

    @Test
    @SuppressWarnings("unchecked")
    void readStructShouldRethrow() throws PackstreamReaderException {
        var registry = mock(StructRegistry.class);
        var reader = mock(StructReader.class);

        var buf = prepareBuffer(
                b -> b.writeByte(TypeMarker.TINY_STRUCT.getValue()).writeByte(0x42));

        when(registry.getReader(any())).thenReturn(Optional.of(reader));
        when(reader.read(isNull(), eq(buf), any())).thenThrow(new PackstreamReaderException("Test Exception"));

        var ex = assertThrows(PackstreamReaderException.class, () -> buf.readStruct(null, registry));

        assertThat(ex.getMessage()).isEqualTo("Test Exception");

        verify(registry).getReader(notNull());
        verifyNoMoreInteractions(registry);

        verify(reader).read(isNull(), eq(buf), notNull());
        verifyNoMoreInteractions(reader);
    }
}
