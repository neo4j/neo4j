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
package org.neo4j.kernel.api.database.enrichment;

import static java.time.ZoneOffset.UTC;
import static org.neo4j.util.Preconditions.checkArgument;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.neo4j.storageengine.api.enrichment.WriteEnrichmentChannel;
import org.neo4j.values.AnyValue;
import org.neo4j.values.AnyValueWriter;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.utils.TemporalUtil;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;

/**
 * @param channel the channel to write the {@link Value} objects out to.
 */
public record ValuesWriter(WriteEnrichmentChannel channel) implements AnyValueWriter<RuntimeException> {

    public int write(AnyValue value) {
        final var position = channel.size();
        if (value == null) {
            channel.put(ValuesReader.NO_VALUE.id());
        } else {
            channel.put(ValuesReader.forValueClass(value.getClass()).id());

            if (value instanceof ListValue list) {
                writeList(list);
            } else if (value instanceof MapValue map) {
                writeMap(map);
            } else {
                value.writeTo(this);
            }
        }
        return position;
    }

    @Override
    public EntityMode entityMode() {
        return EntityMode.FULL;
    }

    @Override
    public void writeNull() {
        // no-op
    }

    @Override
    public void writeBoolean(boolean value) {
        channel.put((byte) (value ? 1 : 0));
    }

    @Override
    public void writeInteger(byte value) {
        channel.put(value);
    }

    @Override
    public void writeInteger(short value) {
        channel.putShort(value);
    }

    @Override
    public void writeInteger(int value) {
        channel.putInt(value);
    }

    @Override
    public void writeInteger(long value) {
        channel.putLong(value);
    }

    @Override
    public void writeFloatingPoint(float value) {
        channel.putFloat(value);
    }

    @Override
    public void writeFloatingPoint(double value) {
        channel.putDouble(value);
    }

    @Override
    public void writeString(String value) {
        if (value == null) {
            channel.putInt(-1);
        } else {
            final var bytes = value.getBytes(StandardCharsets.UTF_8);
            channel.putInt(bytes.length).put(bytes);
        }
    }

    @Override
    public void writeString(char value) {
        channel.putChar(value);
    }

    @Override
    public void beginArray(int size, ArrayType arrayType) {
        channel.putInt(size);
    }

    @Override
    public void endArray() {
        // no-op
    }

    @Override
    public void writeByteArray(byte[] value) {
        channel.putInt(value.length);
        channel.put(value);
    }

    @Override
    public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) {
        checkArgument(
                coordinate.length == crs.getDimension(),
                "Dimension for %s is %d, got %d",
                crs.getName(),
                crs.getDimension(),
                coordinate.length);
        channel.putInt(crs.getCode());
        for (var i = 0; i < crs.getDimension(); i++) {
            channel.putDouble(coordinate[i]);
        }
    }

    @Override
    public void writeDuration(long months, long days, long seconds, int nanos) {
        channel.putLong(months);
        channel.putLong(days);
        channel.putLong(seconds);
        channel.putInt(nanos);
    }

    @Override
    public void writeDate(LocalDate localDate) {
        channel.putLong(localDate.toEpochDay());
    }

    @Override
    public void writeLocalTime(LocalTime localTime) {
        channel.putLong(localTime.toNanoOfDay());
    }

    @Override
    public void writeTime(OffsetTime offsetTime) {
        channel.putLong(TemporalUtil.getNanosOfDayUTC(offsetTime));
        channel.putInt(offsetTime.getOffset().getTotalSeconds());
    }

    @Override
    public void writeLocalDateTime(LocalDateTime localDateTime) {
        channel.putLong(localDateTime.toEpochSecond(UTC));
        channel.putInt(localDateTime.getNano());
    }

    @Override
    public void writeDateTime(ZonedDateTime zonedDateTime) {
        channel.putLong(zonedDateTime.toEpochSecond());
        channel.putInt(zonedDateTime.getNano());

        final var zone = zonedDateTime.getZone();
        if (zone instanceof ZoneOffset zoneOffset) {
            final var offsetSeconds = zoneOffset.getTotalSeconds();
            // lowest bit set to 0: it's a zone offset in seconds
            channel.putInt(offsetSeconds << 1);
        } else {
            // lowest bit set to 1: it's a zone id
            final var zoneId = (TimeZones.map(zone.getId()) << 1) | 1;
            channel.putInt(zoneId);
        }
    }

    @Override
    public void beginMap(int size) {
        channel.putInt(size);
    }

    @Override
    public void endMap() {
        // NOOP - ValuesWriter does not feature explicit termination
    }

    @Override
    public void beginList(int size) {
        channel.putInt(size);
    }

    @Override
    public void endList() {
        // NOOP - ValuesWriter does not feature explicit termination
    }

    @Override
    public void writePath(NodeValue[] nodes, RelationshipValue[] relationships) {
        writeBoolean(true);
        channel.putInt(nodes.length);
        for (var node : nodes) {
            node.writeTo(this);
        }

        for (var relationship : relationships) {
            relationship.writeTo(this);
        }
    }

    @Override
    public void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted) {
        writeBoolean(true);
        writeInteger(nodeId);
        writeString(elementId);
        writeBoolean(isDeleted);

        writeInteger(labels.intSize());
        for (var i = 0; i < labels.intSize(); i++) {
            writeString(labels.stringValue(i));
        }

        beginMap(properties.size());
        properties.foreach((key, value) -> {
            writeString(key);
            write(value);
        });
        endMap();
    }

    @Override
    public void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted)
            throws RuntimeException {
        writeBoolean(true);
        writeInteger(relId);
        writeString(elementId);
        writeString(type.stringValue());
        writeBoolean(isDeleted);

        writeInteger(startNodeId);
        writeString(startNodeElementId);
        writeInteger(endNodeId);
        writeString(endNodeElementId);

        beginMap(properties.size());
        properties.foreach((key, value) -> {
            writeString(key);
            write(value);
        });
        endMap();
    }

    @Override
    public void writePathReference(long[] nodes, long[] relationships) {
        writeBoolean(false);
        channel.putInt(nodes.length);
        for (var node : nodes) {
            channel.putLong(node);
        }

        for (var relationship : relationships) {
            channel.putLong(relationship);
        }
    }

    @Override
    public void writePathReference(VirtualNodeValue[] nodes, VirtualRelationshipValue[] relationships) {
        writeBoolean(false);
        channel.putInt(nodes.length);
        for (var node : nodes) {
            channel.putLong(node.id());
        }

        for (var relationship : relationships) {
            channel.putLong(relationship.id());
        }
    }

    @Override
    public void writePathReference(List<VirtualNodeValue> nodes, List<VirtualRelationshipValue> relationships)
            throws RuntimeException {
        writeBoolean(false);
        channel.putInt(nodes.size());
        for (var node : nodes) {
            channel.putLong(node.id());
        }

        for (var relationship : relationships) {
            channel.putLong(relationship.id());
        }
    }

    @Override
    public void writeNodeReference(long nodeId) {
        writeBoolean(false);
        writeInteger(nodeId);
    }

    @Override
    public void writeRelationshipReference(long relId) {
        writeBoolean(false);
        writeInteger(relId);
    }

    private void writeList(ListValue list) {
        beginList(list.intSize());
        switch (list.iterationPreference()) {
            case RANDOM_ACCESS -> {
                for (var i = 0; i < list.actualSize(); i++) {
                    write(list.value(i));
                }
            }
            case ITERATION -> {
                for (var value : list) {
                    write(value);
                }
            }
        }
        endList();
    }

    private void writeMap(MapValue map) {
        beginMap(map.size());
        map.foreach((key, value) -> {
            writeString(key);
            write(value);
        });
        endMap();
    }
}
