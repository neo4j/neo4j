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

import static org.neo4j.values.storable.NoValue.NO_VALUE;

import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.reader.UnexpectedTypeMarkerException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.function.Reader;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UUIDValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Provides utility functions which permit the reading of kernel values from Packstream buffers.
 */
public abstract class AbstractPackstreamValueReader implements PackstreamValueReader {

    protected final PackstreamBuf buf;

    public AbstractPackstreamValueReader(PackstreamBuf buf) {
        this.buf = buf;
    }

    @Override
    public AnyValue readValue() throws PackstreamReaderException {
        return this.readValue(this.buf.peekType());
    }

    protected AnyValue readValue(Type type) throws PackstreamReaderException {
        return switch (type) {
            case STRUCT -> this.readStruct();
            case LIST -> this.readList();
            case MAP -> this.readMap();
            default -> this.readPrimitiveValue(type, -1);
        };
    }

    @Override
    public AnyValue readPrimitiveValue(long limit) throws PackstreamReaderException {
        return this.readPrimitiveValue(buf.peekType(), limit);
    }

    protected AnyValue readPrimitiveValue(Type type, long limit) throws PackstreamReaderException {
        return switch (type) {
            case NONE -> this.readNull();
            case BYTES -> this.readByteArray(limit);
            case BOOLEAN -> this.readBoolean();
            case FLOAT -> this.readDouble();
            case INT -> this.readLong();
            case LIST -> this.readPrimitiveList(limit);
            case MAP -> this.readPrimitiveMap(limit);
            case STRING -> this.readText(limit);
            case UUID -> this.readUUID();
            default ->
                throw UnexpectedTypeException.wrongType(
                        // DRI-030
                        String.valueOf(limit), Type.VALID_TYPE_NAMES, type);
        };
    }

    @Override
    public NoValue readNull() throws UnexpectedTypeMarkerException {
        buf.readNull();
        return NO_VALUE;
    }

    @Override
    public BooleanValue readBoolean() throws UnexpectedTypeException {
        return Values.booleanValue(this.buf.readBoolean());
    }

    @Override
    public LongValue readLong() throws UnexpectedTypeException {
        return Values.longValue(this.buf.readInt());
    }

    @Override
    public DoubleValue readDouble() throws UnexpectedTypeMarkerException {
        return Values.doubleValue(this.buf.readFloat64());
    }

    @Override
    public ByteArray readByteArray(long limit) throws PackstreamReaderException {
        var payload = this.buf.readBytes(limit);

        var heap = new byte[payload.readableBytes()];
        payload.readBytes(heap);

        return Values.byteArray(heap);
    }

    @Override
    public TextValue readText(long limit) throws PackstreamReaderException {
        return Values.stringValue(this.buf.readString(limit));
    }

    @Override
    public UUIDValue readUUID() throws PackstreamReaderException {
        return Values.uuidValue(this.buf.readUUID());
    }

    @Override
    public ListValue readList() throws PackstreamReaderException {
        return VirtualValues.fromList(buf.readList(buf -> this.readValue()));
    }

    @Override
    public ListValue readPrimitiveList(long limit) throws PackstreamReaderException {
        return VirtualValues.fromList(buf.readList(limit, buf -> this.readPrimitiveValue(limit)));
    }

    @Override
    public MapValue readMap() throws PackstreamReaderException {
        return this.readMap(-1, buf -> this.readValue());
    }

    @Override
    public MapValue readPrimitiveMap(long limit) throws PackstreamReaderException {
        return readMap(limit, buf -> this.readPrimitiveValue(limit));
    }

    protected MapValue readMap(long limit, Reader<AnyValue> reader) throws PackstreamReaderException {
        var map = buf.readMap(limit, reader);
        if (map.isEmpty()) {
            return MapValue.EMPTY;
        }

        // TODO: Refactor - Duplicate map construction
        var builder = new MapValueBuilder(map.size());
        map.forEach(builder::add);
        return builder.build();
    }
}
