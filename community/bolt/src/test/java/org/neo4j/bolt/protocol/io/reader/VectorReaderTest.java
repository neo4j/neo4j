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
package org.neo4j.bolt.protocol.io.reader;

import static io.netty.buffer.Unpooled.buffer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.neo4j.packstream.io.TypeMarker.FLOAT32;
import static org.neo4j.packstream.io.TypeMarker.FLOAT64;
import static org.neo4j.packstream.io.TypeMarker.INT16;
import static org.neo4j.packstream.io.TypeMarker.INT32;
import static org.neo4j.packstream.io.TypeMarker.INT8;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.io.reader.struct.VectorReader;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.TypeMarker;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

public class VectorReaderTest {

    private <V extends VectorValue> void assertValue(PackstreamBuf buf, Class<V> type, V expected)
            throws PackstreamReaderException {
        var value = read(buf);

        assertThat(value)
                .asInstanceOf(type(type))
                .satisfies(input -> assertThat(input).isEqualTo(expected));
    }

    private VectorValue read(PackstreamBuf buf) throws PackstreamReaderException {
        return VectorReader.getInstance().read(null, buf, new StructHeader(2, (short) 'V'));
    }

    private PackstreamBuf bufferFor(TypeMarker marker) {
        return PackstreamBuf.allocUnpooled().writeBytes(Unpooled.wrappedBuffer(new byte[] {(byte) marker.getValue()}));
    }

    @Test
    void shouldRead8Vector() throws PackstreamReaderException {
        var buffer =
                bufferFor(INT8).writeBytes(buffer().writeByte(0).writeByte(42).writeByte(21));

        assertValue(buffer, Int8Vector.class, Values.int8Vector((byte) 0, (byte) 42, (byte) 21));
    }

    @Test
    void shouldRead16Vector() throws PackstreamReaderException {
        var buffer = bufferFor(INT16)
                .writeBytes(buffer().writeShort(0).writeShort(42).writeShort(21));

        assertValue(buffer, Int16Vector.class, Values.int16Vector((short) 0, (short) 42, (short) 21));
    }

    @Test
    void shouldRead32Vector() throws PackstreamReaderException {
        var buffer =
                bufferFor(INT32).writeBytes(buffer().writeInt(0).writeInt(42).writeInt(21));

        assertValue(buffer, Int32Vector.class, Values.int32Vector(0, 42, 21));
    }

    @Test
    void shouldReadFloat32Vector() throws PackstreamReaderException {
        var buffer = bufferFor(FLOAT32)
                .writeBytes(buffer().writeFloat(0.125f).writeFloat(42.25f).writeFloat(21.5f));

        assertValue(buffer, Float32Vector.class, Values.float32Vector(0.125f, 42.25f, 21.5f));
    }

    @Test
    void shouldFailIfFloat32VectorContainsNonFiniteValues() {
        assertThatThrownBy(() -> read(bufferFor(FLOAT32).writeBytes(buffer().writeFloat(Float.NEGATIVE_INFINITY))))
                .isInstanceOf(InvalidArgumentException.class);
        assertThatThrownBy(() -> read(bufferFor(FLOAT32).writeBytes(buffer().writeFloat(Float.POSITIVE_INFINITY))))
                .isInstanceOf(InvalidArgumentException.class);
        assertThatThrownBy(() -> read(bufferFor(FLOAT32).writeBytes(buffer().writeFloat(Float.NaN))))
                .isInstanceOf(InvalidArgumentException.class);
    }

    @Test
    void shouldReadFloat64Vector() throws PackstreamReaderException {
        var buffer = bufferFor(FLOAT64)
                .writeBytes(buffer().writeDouble(0.125f).writeDouble(42.25f).writeDouble(21.5f));

        assertValue(buffer, Float64Vector.class, Values.float64Vector(0.125f, 42.25f, 21.5f));
    }

    @Test
    void shouldFailIfFloat64VectorContainsNonFiniteValues() {
        assertThatThrownBy(() -> read(bufferFor(FLOAT64).writeBytes(buffer().writeDouble(Double.NEGATIVE_INFINITY))))
                .isInstanceOf(InvalidArgumentException.class);
        assertThatThrownBy(() -> read(bufferFor(FLOAT64).writeBytes(buffer().writeDouble(Double.POSITIVE_INFINITY))))
                .isInstanceOf(InvalidArgumentException.class);
        assertThatThrownBy(() -> read(bufferFor(FLOAT64).writeBytes(buffer().writeDouble(Double.NaN))))
                .isInstanceOf(InvalidArgumentException.class);
    }
}
