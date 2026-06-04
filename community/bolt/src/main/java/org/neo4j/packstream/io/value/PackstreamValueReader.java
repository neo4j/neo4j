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

import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.reader.UnexpectedTypeException;
import org.neo4j.packstream.error.reader.UnexpectedTypeMarkerException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.ByteArray;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NoValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.UUIDValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public interface PackstreamValueReader {

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    AnyValue readValue() throws PackstreamReaderException;

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    default AnyValue readPrimitiveValue() throws PackstreamReaderException {
        return this.readPrimitiveValue(-1);
    }

    /**
     * Decodes an arbitrary native Packstream value from a given buffer.
     *
     * @param limit a size constraint for the permitted values.
     * @return a value.
     * @throws PackstreamReaderException when a given value is malformed or exceeds a limit.
     */
    AnyValue readPrimitiveValue(long limit) throws PackstreamReaderException;

    /**
     * Decodes a null value from a given buffer.
     *
     * @return a null value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    NoValue readNull() throws UnexpectedTypeMarkerException;

    /**
     * Decodes a boolean value from a given buffer.
     *
     * @return a boolean value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    BooleanValue readBoolean() throws UnexpectedTypeException;

    /**
     * Decodes a long value from a given buffer.
     *
     * @return a long value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    LongValue readLong() throws UnexpectedTypeException;

    /**
     * Decodes a double value from a given buffer.
     *
     * @return a double value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     */
    DoubleValue readDouble() throws UnexpectedTypeMarkerException;

    /**
     * Decodes a byte array value from a given buffer.
     *
     * @return a byte array value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    default ByteArray readByteArray() throws PackstreamReaderException {
        return this.readByteArray(-1);
    }

    /**
     * Decodes a byte array value from a given buffer.
     *
     * @return a byte array value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    ByteArray readByteArray(long limit) throws PackstreamReaderException;

    /**
     * Decodes a text value from a given buffer.
     *
     * @return a text value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    default TextValue readText() throws PackstreamReaderException {
        return this.readText(-1);
    }

    /**
     * Decodes a text value from a given buffer.
     *
     * @return a text value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    TextValue readText(long limit) throws PackstreamReaderException;

    /**
     * Decodes a UUID value from a given buffer.
     *
     * @return a UUID value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException when the value is malformed.
     */
    UUIDValue readUUID() throws PackstreamReaderException;

    /**
     * Decodes a list value from a given buffer.
     *
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    ListValue readList() throws PackstreamReaderException;

    /**
     * Decodes a list value consisting of primitive values from a given buffer.
     *
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    default ListValue readPrimitiveList() throws PackstreamReaderException {
        return this.readPrimitiveList(-1);
    }

    /**
     * Decodes a list value consisting of primitive values from a given buffer.
     *
     * @return a list value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    ListValue readPrimitiveList(long limit) throws PackstreamReaderException;

    /**
     * Decodes a map value from a given buffer.
     *
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    MapValue readMap() throws PackstreamReaderException;

    /**
     * Decodes a map value consisting of primitive values from a given buffer.
     *
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    default MapValue readPrimitiveMap() throws PackstreamReaderException {
        return this.readPrimitiveMap(-1);
    }

    /**
     * Decodes a map value consisting of primitive values from a given buffer.
     *
     * @return a map value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    MapValue readPrimitiveMap(long limit) throws PackstreamReaderException;

    /**
     * Decodes a struct value from a given buffer.
     *
     * @return a struct value.
     * @throws UnexpectedTypeMarkerException when an unexpected type marker is encountered.
     * @throws PackstreamReaderException     when the value is malformed.
     */
    Value readStruct() throws PackstreamReaderException;
}
