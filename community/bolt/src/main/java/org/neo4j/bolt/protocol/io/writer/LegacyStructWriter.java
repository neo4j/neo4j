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
package org.neo4j.bolt.protocol.io.writer;

import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static org.neo4j.bolt.protocol.io.StructType.DATE_TIME_LEGACY;
import static org.neo4j.bolt.protocol.io.StructType.DATE_TIME_ZONE_ID_LEGACY;
import static org.neo4j.bolt.protocol.io.StructType.NODE;
import static org.neo4j.bolt.protocol.io.StructType.RELATIONSHIP;
import static org.neo4j.bolt.protocol.io.StructType.UNBOUND_RELATIONSHIP;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;

/**
 * Provides support for legacy Bolt structures.
 * <p />
 * This writer implementation is present on all 4.x Bolt connections and facilitates backwards compatibility with prior
 * specifications of the structures provided by Bolt.
 *
 * @deprecated Scheduled for removal in 6.0 - Support for 4.x drivers will be dropped entirely
 */
@Deprecated(since = "5.0", forRemoval = true)
public final class LegacyStructWriter implements StructWriter {
    private static final LegacyStructWriter INSTANCE = new LegacyStructWriter();

    private LegacyStructWriter() {}

    public static StructWriter getInstance() {
        return INSTANCE;
    }

    @Override
    public void writeDateTime(WriterContext ctx, ZonedDateTime zonedDateTime) {
        requireNonNull(zonedDateTime, "dateTime cannot be null");
        var epochSecondLocal = zonedDateTime.toLocalDateTime().toEpochSecond(UTC);

        var zone = zonedDateTime.getZone();

        if (zone instanceof ZoneOffset) {
            DATE_TIME_LEGACY.writeHeader(ctx);
            ctx.buffer()
                    .writeInt(epochSecondLocal)
                    .writeInt(zonedDateTime.getNano())
                    .writeInt(zonedDateTime.getOffset().getTotalSeconds());
            return;
        }

        DATE_TIME_ZONE_ID_LEGACY.writeHeader(ctx);
        ctx.buffer()
                .writeInt(epochSecondLocal)
                .writeInt(zonedDateTime.getNano())
                .writeString(zone.getId());
    }

    @Override
    public void writeNode(
            WriterContext ctx,
            String elementId,
            long nodeId,
            TextArray labels,
            MapValue properties,
            boolean isDeleted) {
        NODE.writeLegacyHeader(ctx);

        var stringLabels = StreamSupport.stream(labels.spliterator(), false)
                .map(label -> ((TextValue) label).stringValue())
                .collect(Collectors.toList());

        ctx.buffer().writeInt(nodeId).writeList(stringLabels, PackstreamBuf::writeString);
        ctx.writeValue(properties);
    }

    @Override
    public void writeRelationship(
            WriterContext ctx,
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted) {
        RELATIONSHIP.writeLegacyHeader(ctx);

        ctx.buffer().writeInt(relId).writeInt(startNodeId).writeInt(endNodeId).writeString(type.stringValue());
        ctx.writeValue(properties);
    }

    /**
     * Encodes an unbound relationship structure to a given buffer.
     *
     * @param relId      a relationship id.
     * @param type       a type string.
     * @param properties a set of properties.
     */
    @Override
    public void writeUnboundRelationship(
            WriterContext ctx, String elementId, long relId, String type, MapValue properties) {
        UNBOUND_RELATIONSHIP.writeLegacyHeader(ctx);

        ctx.buffer().writeInt(relId).writeString(type);
        ctx.writeValue(properties);
    }
}
