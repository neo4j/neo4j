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
package org.neo4j.packstream.io.value;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.values.storable.NoValue.NO_VALUE;

import io.netty.buffer.Unpooled;
import java.util.Collections;
import java.util.List;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.reader.UnexpectedTypeMarkerException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

class PackstreamValueReaderTest {

    @Test
    void readPrimitiveValueShouldFailWithUnexpectedTypeWhenStructIsGiven() {
        var buf = PackstreamBuf.allocUnpooled().writeStructHeader(new StructHeader(2, (short) 42));

        var reader = new PackstreamValueReader<>(null, buf, null);
        assertThatThrownBy(() -> reader.readPrimitiveValue(-1))
                .isInstanceOf(UnexpectedTypeException.class)
                .hasMessage("Unexpected type: STRUCT");
    }

    @Test
    void shouldReadNull() throws UnexpectedTypeMarkerException {
        var buf = PackstreamBuf.allocUnpooled().writeNull();

        var reader = new PackstreamValueReader<>(null, buf, null);
        var value = reader.readNull();

        assertThat(value).isSameAs(NO_VALUE);

        assertThat(buf.getTarget().isReadable()).isFalse();
    }

    @TestFactory
    Stream<DynamicTest> shouldReadBoolean() {
        return Stream.of(false, true)
                .map(expected -> dynamicTest(Boolean.toString(expected), () -> {
                    var buf = PackstreamBuf.allocUnpooled().writeBoolean(expected);

                    var reader = new PackstreamValueReader<>(null, buf, null);
                    var actual = reader.readBoolean();

                    assertThat(actual.booleanValue()).isEqualTo(expected);

                    assertThat(buf.getTarget().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadFloat() {
        return DoubleStream.of(21.125, 42.25, 84.5, 168)
                .mapToObj(expected -> dynamicTest(Double.toString(expected), () -> {
                    var buf = PackstreamBuf.allocUnpooled().writeFloat(expected);

                    var reader = new PackstreamValueReader<>(null, buf, null);
                    var actual = reader.readDouble();

                    assertThat(actual.value()).isEqualTo(expected);

                    assertThat(buf.getTarget().isReadable()).isFalse();
                }));
    }

    @TestFactory
    Stream<DynamicTest> shouldReadByteArray() {
        return IntStream.range(0, 128).mapToObj(size -> {
            var payload = new byte[size];
            for (var i = 0; i < payload.length; ++i) {
                payload[i] = (byte) (i % Byte.MAX_VALUE);
            }

            return dynamicTest(String.format("%d elements", size), () -> {
                var buf = PackstreamBuf.allocUnpooled().writeBytes(Unpooled.wrappedBuffer(payload));

                var reader = new PackstreamValueReader<>(null, buf, null);
                var actual = reader.readByteArray();

                assertThat(actual.asObjectCopy()).isEqualTo(payload);

                assertThat(buf.getTarget().isReadable()).isFalse();
            });
        });
    }

    @TestFactory
    Stream<DynamicTest> shouldReadText() {
        return IntStream.range(0, 32).mapToObj(size -> {
            var payload = "A".repeat(size);

            return dynamicTest(String.format("%d characters", size), () -> {
                var buf = PackstreamBuf.allocUnpooled().writeString(payload);

                var reader = new PackstreamValueReader<>(null, buf, null);
                var actual = reader.readText();

                assertThat(actual.stringValue()).isEqualTo(payload);

                assertThat(buf.getTarget().isReadable()).isFalse();
            });
        });
    }

    @TestFactory
    Stream<DynamicTest> shouldReadList() {
        return Stream.of(
                        NO_VALUE,
                        Values.byteArray(new byte[] {21, 42, 84}),
                        Values.booleanValue(true),
                        Values.doubleValue(42.25),
                        Values.intValue(42),
                        Values.stringValue("B0L7"))
                .map(payload -> dynamicTest(payload.toString(), () -> {
                    var values = Collections.nCopies(5, payload);

                    @SuppressWarnings({"unchecked", "rawtypes"})
                    var expected = VirtualValues.fromList((List) values);

                    var buf = PackstreamBuf.allocUnpooled().writeList(values, (b, val) -> {
                        if (val instanceof ByteArray ba) {
                            b.writeBytes(Unpooled.wrappedBuffer(ba.asObjectCopy()));
                        } else if (val instanceof BooleanValue bo) {
                            b.writeBoolean(bo.booleanValue());
                        } else if (val instanceof DoubleValue db) {
                            b.writeFloat(db.doubleValue());
                        } else if (val instanceof IntValue i) {
                            b.writeInt(i.intValue());
                        } else if (val instanceof StringValue str) {
                            b.writeString(str.stringValue());
                        } else if (val == NO_VALUE) {
                            b.writeNull();
                        }
                    });

                    var reader = new PackstreamValueReader<>(null, buf, null);
                    var actual = reader.readList();

                    assertThat(actual).isEqualTo(expected);
                }));
    }

    @Test
    void readPrimitiveListShouldFailWithUnexpectedTypeWhenStructIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeListHeader(3)
                .writeNull()
                .writeStructHeader(new StructHeader(3, (short) 42))
                .writeString("foo");

        var reader = new PackstreamValueReader<>(null, buf, null);
        assertThatThrownBy(() -> reader.readPrimitiveList(-1))
                .isInstanceOf(UnexpectedTypeException.class)
                .hasMessage("Unexpected type: STRUCT");
    }

    @Test
    void shouldReadMap() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled();

        var elements = new MapValueBuilder();

        elements.add("value0", NO_VALUE);
        elements.add("value1", Values.byteArray(new byte[] {21, 42, 84}));
        elements.add("value2", Values.booleanValue(true));
        elements.add("value3", Values.doubleValue(42.25));
        elements.add("value4", Values.intValue(42));
        elements.add("value5", Values.stringValue("B0L7"));

        var expected = elements.build();

        buf.writeMapHeader(expected.size());
        expected.foreach((key, val) -> {
            buf.writeString(key);

            if (val instanceof ByteArray ba) {
                buf.writeBytes(Unpooled.wrappedBuffer(ba.asObjectCopy()));
            } else if (val instanceof BooleanValue bo) {
                buf.writeBoolean(bo.booleanValue());
            } else if (val instanceof DoubleValue db) {
                buf.writeFloat(db.doubleValue());
            } else if (val instanceof IntValue i) {
                buf.writeInt(i.intValue());
            } else if (val instanceof StringValue str) {
                buf.writeString(str.stringValue());
            } else if (val == NO_VALUE) {
                buf.writeNull();
            }
        });

        var reader = new PackstreamValueReader<>(null, buf, null);
        var actual = reader.readMap();

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void readMapShouldHandleEmptyMap() throws PackstreamReaderException {
        var buf = PackstreamBuf.allocUnpooled().writeMapHeader(0);

        var reader = new PackstreamValueReader<>(null, buf, null);
        var map = reader.readMap();

        assertThat(map).isSameAs(MapValue.EMPTY);
    }

    @Test
    void readPrimitiveMapShouldFailWithUnexpectedTypeWhenStructIsGiven() {
        var buf = PackstreamBuf.allocUnpooled()
                .writeMapHeader(3)
                .writeString("foo")
                .writeString("bar")
                .writeString("broken")
                .writeStructHeader(new StructHeader(1, (short) 42))
                .writeString("answer")
                .writeInt(42);

        var reader = new PackstreamValueReader<>(null, buf, null);
        assertThatThrownBy(() -> reader.readPrimitiveMap(-1))
                .isInstanceOf(UnexpectedTypeException.class)
                .hasMessage("Unexpected type: STRUCT");
    }
}
