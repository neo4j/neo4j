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
package org.neo4j.bolt.protocol.io.reader.struct;

import java.util.List;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.packstream.error.reader.LimitExceededException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.TypeMarker;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.Float32Vector;
import org.neo4j.values.storable.Float64Vector;
import org.neo4j.values.storable.Int16Vector;
import org.neo4j.values.storable.Int32Vector;
import org.neo4j.values.storable.Int64Vector;
import org.neo4j.values.storable.Int8Vector;
import org.neo4j.values.storable.Values;
import org.neo4j.values.storable.VectorValue;

public final class VectorReader<CTX> implements StructReader<CTX, VectorValue> {
    private static final VectorReader INSTANCE = new VectorReader();

    private VectorReader() {}

    @SuppressWarnings("unchecked")
    public static <CTX> VectorReader<CTX> getInstance() {
        return (VectorReader<CTX>) INSTANCE;
    }

    @Override
    public short getTag() {
        return StructType.VECTOR.getTag();
    }

    @Override
    public VectorValue read(Object o, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        var tagLength = buffer.readLengthPrefixMarker(Type.BYTES);
        if (tagLength != 1) {
            throw LimitExceededException.protocolMessageLengthLimitOverflow(1, tagLength);
        }

        var tag = TypeMarker.byEncoded(buffer.raw().readUnsignedByte());

        var length = buffer.readLengthPrefixMarker(Type.BYTES);
        if (length > Integer.MAX_VALUE) {
            throw LimitExceededException.protocolMessageLengthLimitOverflow(Integer.MAX_VALUE, length);
        }

        return switch (tag) {
            case INT8 -> this.readInt8Vector(buffer, (int) length);
            case INT16 -> this.readInt16Vector(buffer, (int) length);
            case INT32 -> this.readInt32Vector(buffer, (int) length);
            case INT64 -> this.readInt64Vector(buffer, (int) length);
            case FLOAT32 -> this.readFloat32Vector(buffer, (int) length);
            case FLOAT64 -> this.readFloat64Vector(buffer, (int) length);
            default ->
                throw IllegalStructArgumentException.wrongTypeForFieldName(
                        "type",
                        Short.toString(tag.getValue()),
                        List.of("INT8", "INT16", "INT32", "INT64", "FLOAT32", "FLOAT64"),
                        tag.name(),
                        "Given type is not a valid vector value type");
        };
    }

    private Int8Vector readInt8Vector(PackstreamBuf buffer, int length) {
        var coordinates = new byte[length];
        buffer.raw().readBytes(coordinates);

        return Values.int8Vector(coordinates);
    }

    private Int16Vector readInt16Vector(PackstreamBuf buffer, int length) {
        // values take up two bytes each so bit shifting by one gives us the actual length without
        // requiring division
        var actualLength = length >>> 1;

        var coordinates = new short[actualLength];
        for (var i = 0; i < actualLength; i++) {
            coordinates[i] = buffer.raw().readShort();
        }

        return Values.int16Vector(coordinates);
    }

    private Int32Vector readInt32Vector(PackstreamBuf buffer, int length) {
        var actualLength = length >>> 2;

        var coordinates = new int[actualLength];
        for (var i = 0; i < actualLength; i++) {
            coordinates[i] = buffer.raw().readInt();
        }

        return Values.int32Vector(coordinates);
    }

    private Int64Vector readInt64Vector(PackstreamBuf buffer, int length) {
        var actualLength = length >>> 3;

        var coordinates = new long[actualLength];
        for (var i = 0; i < actualLength; i++) {
            coordinates[i] = buffer.raw().readLong();
        }

        return Values.int64Vector(coordinates);
    }

    private Float32Vector readFloat32Vector(PackstreamBuf buffer, int length) {
        var actualLength = length >>> 2;

        var coordinates = new float[actualLength];
        var raw = buffer.raw();
        for (var i = 0; i < actualLength; i++) {
            coordinates[i] = raw.readFloat();
        }

        return Values.float32Vector(coordinates);
    }

    private Float64Vector readFloat64Vector(PackstreamBuf buffer, int length) {
        var actualLength = length >>> 3;

        var coordinates = new double[actualLength];
        var raw = buffer.raw();
        for (var i = 0; i < actualLength; i++) {
            coordinates[i] = raw.readDouble();
        }

        return Values.float64Vector(coordinates);
    }
}
