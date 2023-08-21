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
package org.neo4j.packstream.testing;

import org.neo4j.bolt.protocol.io.reader.DateReader;
import org.neo4j.bolt.protocol.io.reader.DateTimeReader;
import org.neo4j.bolt.protocol.io.reader.DateTimeZoneIdReader;
import org.neo4j.bolt.protocol.io.reader.DurationReader;
import org.neo4j.bolt.protocol.io.reader.LocalDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.LocalTimeReader;
import org.neo4j.bolt.protocol.io.reader.Point2dReader;
import org.neo4j.bolt.protocol.io.reader.Point3dReader;
import org.neo4j.bolt.protocol.io.reader.TimeReader;
import org.neo4j.bolt.protocol.io.reader.legacy.LegacyDateTimeReader;
import org.neo4j.bolt.protocol.io.reader.legacy.LegacyDateTimeZoneIdReader;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.io.Type;
import org.neo4j.packstream.struct.StructRegistry;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;

public final class PackstreamTestValueReader {

    /**
     * Defines a set of default structs which are typically accessible within the Bolt protocol.
     * <p />
     * Note that this list only contains non-legacy versions of the decoders which are technically part of the Bolt
     * specification rather than being part of Packstream itself (hence why they are versioned). Some tests may wish to
     * pass their own replacements in order to validate legacy protocol version support.
     */
    public static final StructRegistry<Object, Value> DEFAULT_STRUCT_REGISTRY = StructRegistry.<Object, Value>builder()
            .register(DateReader.getInstance())
            .register(DateTimeReader.getInstance())
            .register(DateTimeZoneIdReader.getInstance())
            .register(DurationReader.getInstance())
            .register(LocalDateTimeReader.getInstance())
            .register(LocalTimeReader.getInstance())
            .register(Point2dReader.getInstance())
            .register(Point3dReader.getInstance())
            .register(TimeReader.getInstance())
            .register(LegacyDateTimeReader.getInstance()) // Legacy types use different types
            .register(LegacyDateTimeZoneIdReader.getInstance())
            .build();

    private PackstreamTestValueReader() {}

    public static Object readValue(PackstreamBuf buf, StructRegistry<?, Value> structRegistry)
            throws PackstreamReaderException {
        var type = buf.peekType();

        return switch (type) {
            case RESERVED -> throw new AssertionError("Encountered reserved type tag");
            case NONE -> null;
            case BYTES -> buf.readBytes();
            case BOOLEAN -> buf.readBoolean();
            case FLOAT -> buf.readFloat();
            case INT -> buf.readInt();
            case LIST -> buf.readList(ignore -> readValue(buf, structRegistry));
            case MAP -> buf.readMap(ignore -> readValue(buf, structRegistry));
            case STRING -> buf.readString();
            case STRUCT -> buf.readStruct(null, structRegistry);
        };
    }

    public static AnyValue readStorable(PackstreamBuf buf, StructRegistry<?, Value> structRegistry)
            throws PackstreamReaderException {
        var type = buf.peekType();

        return switch (type) {
            case RESERVED -> throw new AssertionError("Encountered reserved type tag");
            case NONE -> {
                buf.readNull();
                yield Values.NO_VALUE;
            }
            case BYTES -> {
                var buffer = buf.readBytes();

                var heap = new byte[buffer.readableBytes()];
                buffer.readBytes(heap);

                yield Values.byteArray(heap);
            }
            case BOOLEAN -> Values.booleanValue(buf.readBoolean());
            case FLOAT -> Values.doubleValue(buf.readFloat());
            case INT -> Values.longValue(buf.readInt());
            case LIST -> readListValue(buf, structRegistry);
            case MAP -> readMapValue(buf, structRegistry);
            case STRING -> Values.stringValue(buf.readString());
            case STRUCT -> buf.readStruct(null, structRegistry);
        };
    }

    public static ListValue readListValue(PackstreamBuf buf, StructRegistry<?, Value> structRegistry)
            throws PackstreamReaderException {
        var length = buf.readLengthPrefixMarker(Type.LIST);
        var builder = ListValueBuilder.newListBuilder((int) length);
        for (var i = 0; i < length; ++i) {
            builder.add(readStorable(buf, structRegistry));
        }

        return builder.build();
    }

    public static MapValue readMapValue(PackstreamBuf buf, StructRegistry<?, Value> structRegistry)
            throws PackstreamReaderException {
        var length = buf.readLengthPrefixMarker(Type.MAP);

        if (length == 0) {
            return MapValue.EMPTY;
        }

        var builder = new MapValueBuilder((int) length);
        for (var i = 0; i < length; ++i) {
            builder.add(buf.readString(), readStorable(buf, structRegistry));
        }

        return builder.build();
    }
}
