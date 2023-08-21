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
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Provides utility functions which permit the reading of kernel values from Packstream buffers.
 */
public class PackstreamValueReader<CTX> {
    private final CTX ctx;
    private final PackstreamBuf buf;
    private final StructRegistry<CTX, Value> structRegistry;

    public PackstreamValueReader(CTX ctx, PackstreamBuf buf, StructRegistry<CTX, Value> structRegistry) {
        this.ctx = ctx;
        this.buf = buf;
        this.structRegistry = structRegistry;
    }

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    public AnyValue readValue() throws PackstreamReaderException {
        return this.doReadValue(this.buf.peekType());
    }

    private AnyValue doReadValue(Type type) throws PackstreamReaderException {
        return switch (type) {
            case STRUCT -> this.readStruct();
            case LIST -> this.readList();
            case MAP -> this.readMap();
            default -> this.doReadPrimitiveValue(type, -1);
        };
    }

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    public AnyValue readPrimitiveValue(long limit) throws PackstreamReaderException {
        return this.doReadPrimitiveValue(buf.peekType(), limit);
    }

    private AnyValue doReadPrimitiveValue(Type type, long limit) throws PackstreamReaderException {
        return switch (type) {
            case NONE -> this.readNull();
            case BYTES -> this.readByteArray(limit);
            case BOOLEAN -> this.readBoolean();
            case FLOAT -> this.readDouble();
            case INT -> this.readLong();
            case LIST -> this.readPrimitiveList(limit);
            case MAP -> this.readPrimitiveMap(limit);
            case STRING -> this.readText(limit);
            default -> throw new UnexpectedTypeException(type);
        };
    }

    /**
     * Decodes a null value from a given buffer.
     *
     * @return a null value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public NoValue readNull() throws UnexpectedTypeMarkerException {
        buf.readNull();
        return NO_VALUE;
    }

    /**
     * Decodes a boolean value from a given buffer.
     *
     * @return a boolean value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public BooleanValue readBoolean() throws UnexpectedTypeException {
        return Values.booleanValue(this.buf.readBoolean());
    }

    /**
     * Decodes a long value from a given buffer.
     *
     * @return a long value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public LongValue readLong() throws UnexpectedTypeException {
        return Values.longValue(this.buf.readInt());
    }

    /**
     * Decodes a double value from a given buffer.
     *
     * @return a double value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public DoubleValue readDouble() throws UnexpectedTypeMarkerException {
        return Values.doubleValue(this.buf.readFloat());
    }

    /**
     * Decodes a byte array value from a given buffer.
     *
     * @return a byte array value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public ByteArray readByteArray() throws PackstreamReaderException {
        return this.readByteArray(-1);
    }

    /**
     * Decodes a byte array value from a given buffer.
     *
     * @return a byte array value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public ByteArray readByteArray(long limit) throws PackstreamReaderException {
        var payload = this.buf.readBytes(limit);

        var heap = new byte[payload.readableBytes()];
        payload.readBytes(heap);

        return Values.byteArray(heap);
    }

    /**
     * Decodes a text value from a given buffer.
     *
     * @return a text value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public TextValue readText(long limit) throws PackstreamReaderException {
        return Values.stringValue(this.buf.readString(limit));
    }

    /**
     * Decodes a text value from a given buffer.
     *
     * @return a text value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public TextValue readText() throws PackstreamReaderException {
        return this.readText(-1);
    }

    /**
     * Decodes a list value from a given buffer.
     *
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public ListValue readList() throws PackstreamReaderException {
        return VirtualValues.fromList(buf.readList(buf -> this.readValue()));
    }

    /**
     * Decodes a list value consisting of primitive values from a given buffer.
     *
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public ListValue readPrimitiveList(long limit) throws PackstreamReaderException {
        return VirtualValues.fromList(buf.readList(limit, buf -> this.readPrimitiveValue(limit)));
    }

    /**
     * Decodes a map value from a given buffer.
     *
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public MapValue readMap() throws PackstreamReaderException {
        return this.doReadMap(-1, buf -> this.readValue());
    }

    /**
     * Decodes a map value consisting of primitive values from a given buffer.
     *
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public MapValue readPrimitiveMap(long limit) throws PackstreamReaderException {
        return doReadMap(limit, buf -> this.readPrimitiveValue(limit));
    }

    private MapValue doReadMap(long limit, Reader<AnyValue> reader) throws PackstreamReaderException {
        var map = buf.readMap(limit, reader);
        if (map.isEmpty()) {
            return MapValue.EMPTY;
        }

        // TODO: Refactor - Duplicate map construction
        var builder = new MapValueBuilder(map.size());
        map.forEach(builder::add);
        return builder.build();
    }

    /**
     * Decodes a struct value from a given buffer.
     *
     * @return a struct value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public Value readStruct() throws PackstreamReaderException {
        return buf.readStruct(this.ctx, this.structRegistry);
    }
}
