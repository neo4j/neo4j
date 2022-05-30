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
package org.neo4j.packstream.io.value;

import static org.neo4j.values.storable.NoValue.NO_VALUE;

import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.reader.UnexpectedTypeMarkerException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.io.function.Reader;
import org.neo4j.packstream.struct.NativeStructRegistry;
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
public final class PackstreamValues {
    private PackstreamValues() {}

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @param buf a buffer.
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    public static AnyValue readValue(PackstreamBuf buf) throws PackstreamReaderException {
        return doReadValue(buf, buf.peekType());
    }

    private static AnyValue doReadValue(PackstreamBuf buf, Type type) throws PackstreamReaderException {
        if (type == Type.STRUCT) {
            return readStruct(buf);
        }

        return doReadPrimitiveValue(buf, -1, type);
    }

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @param buf a buffer.
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    public static AnyValue readPrimitiveValue(PackstreamBuf buf) throws PackstreamReaderException {
        return readPrimitiveValue(buf, -1);
    }

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @param buf   a buffer.
     * @param limit a collection size limit.
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    public static AnyValue readPrimitiveValue(PackstreamBuf buf, int limit) throws PackstreamReaderException {
        return doReadPrimitiveValue(buf, limit, buf.peekType());
    }

    private static AnyValue doReadPrimitiveValue(PackstreamBuf buf, int limit, Type type)
            throws PackstreamReaderException {
        return switch (type) {
            case NONE -> readNull(buf);
            case BYTES -> readByteArray(buf);
            case BOOLEAN -> readBoolean(buf);
            case FLOAT -> readDouble(buf);
            case INT -> readLong(buf);
            case LIST -> readList(buf, limit);
            case MAP -> readMap(buf, limit);
            case STRING -> readText(buf);
            default -> throw new UnexpectedTypeException(type);
        };
    }

    /**
     * Writes a given arbitrary storable value to a given buffer.
     *
     * @param buf   a buffer.
     * @param value an arbitrary value.
     */
    public static void writeValue(PackstreamBuf buf, AnyValue value) {
        value.writeTo(new PackstreamValueWriter(buf));
    }

    /**
     * Decodes a null value from a given buffer.
     *
     * @param buf a buffer.
     * @return a null value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public static NoValue readNull(PackstreamBuf buf) throws UnexpectedTypeMarkerException {
        buf.readNull();
        return NO_VALUE;
    }

    /**
     * Decodes a boolean value from a given buffer.
     *
     * @param buf a buffer.
     * @return a boolean value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public static BooleanValue readBoolean(PackstreamBuf buf) throws UnexpectedTypeException {
        return Values.booleanValue(buf.readBoolean());
    }

    /**
     * Decodes a long value from a given buffer.
     *
     * @param buf a buffer.
     * @return a long value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public static LongValue readLong(PackstreamBuf buf) throws UnexpectedTypeException {
        return Values.longValue(buf.readInt());
    }

    /**
     * Decodes a double value from a given buffer.
     *
     * @param buf a buffer.
     * @return a double value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    public static DoubleValue readDouble(PackstreamBuf buf) throws UnexpectedTypeMarkerException {
        return Values.doubleValue(buf.readFloat());
    }

    /**
     * Decodes a byte array value from a given buffer.
     *
     * @param buf a buffer.
     * @return a byte array value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static ByteArray readByteArray(PackstreamBuf buf) throws PackstreamReaderException {
        var payload = buf.readBytes();

        var heap = new byte[payload.readableBytes()];
        payload.readBytes(heap);

        return Values.byteArray(heap);
    }

    /**
     * Decodes a text value from a given buffer.
     *
     * @param buf a buffer.
     * @return a text value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static TextValue readText(PackstreamBuf buf) throws PackstreamReaderException {
        return Values.stringValue(buf.readString());
    }

    /**
     * Decodes a list value from a given buffer.
     *
     * @param buf a buffer.
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static ListValue readList(PackstreamBuf buf) throws PackstreamReaderException {
        return readList(buf, -1);
    }

    /**
     * Decodes a list value from a given buffer.
     *
     * @param buf   a buffer.
     * @param limit the maximum number of elements within the list.
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static ListValue readList(PackstreamBuf buf, int limit) throws PackstreamReaderException {
        return VirtualValues.fromList(buf.readList(limit, PackstreamValues::readValue));
    }

    /**
     * Decodes a list value consisting of primitive values from a given buffer.
     *
     * @param buf a buffer.
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static ListValue readPrimitiveList(PackstreamBuf buf) throws PackstreamReaderException {
        return readPrimitiveList(buf, -1);
    }

    /**
     * Decodes a list value consisting of primitive values from a given buffer.
     *
     * @param buf   a buffer.
     * @param limit the maximum number of values within the list.
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static ListValue readPrimitiveList(PackstreamBuf buf, int limit) throws PackstreamReaderException {
        return VirtualValues.fromList(buf.readList(limit, PackstreamValues::readPrimitiveValue));
    }

    /**
     * Decodes a map value from a given buffer.
     *
     * @param buf a buffer.
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static MapValue readMap(PackstreamBuf buf) throws PackstreamReaderException {
        return readMap(buf, -1);
    }

    /**
     * Decodes a map value from a given buffer.
     *
     * @param buf   a buffer.
     * @param limit the maximum number of values within the map.
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static MapValue readMap(PackstreamBuf buf, int limit) throws PackstreamReaderException {
        return doReadMap(buf, limit, PackstreamValues::readValue);
    }

    /**
     * Decodes a map value consisting of primitive values from a given buffer.
     *
     * @param buf a buffer.
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static MapValue readPrimitiveMap(PackstreamBuf buf) throws PackstreamReaderException {
        return readPrimitiveMap(buf, -1);
    }

    /**
     * Decodes a map value consisting of primitive values from a given buffer.
     *
     * @param buf   a buffer.
     * @param limit the maximum number of values within the map.
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static MapValue readPrimitiveMap(PackstreamBuf buf, int limit) throws PackstreamReaderException {
        return doReadMap(buf, limit, b -> readPrimitiveValue(b, limit));
    }

    private static MapValue doReadMap(PackstreamBuf buf, int limit, Reader<AnyValue> reader)
            throws PackstreamReaderException {
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
     * @param buf a buffer.
     * @return a struct value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    public static Value readStruct(PackstreamBuf buf) throws PackstreamReaderException {
        return buf.readStruct(NativeStructRegistry.getInstance());
    }
}
