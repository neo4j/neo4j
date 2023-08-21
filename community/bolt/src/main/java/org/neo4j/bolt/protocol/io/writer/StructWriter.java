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

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public interface StructWriter {

    default void writePoint(WriterContext ctx, CoordinateReferenceSystem crs, double[] coords) {
        ctx.firePoint(crs, coords);
    }

    default void writeDuration(WriterContext ctx, long months, long days, long seconds, int nanos) {
        ctx.fireDuration(months, days, seconds, nanos);
    }

    default void writeDate(WriterContext ctx, LocalDate localDate) {
        ctx.fireDate(localDate);
    }

    default void writeLocalTime(WriterContext ctx, LocalTime localTime) {
        ctx.fireLocalTime(localTime);
    }

    default void writeTime(WriterContext ctx, OffsetTime offsetTime) {
        ctx.fireTime(offsetTime);
    }

    default void writeLocalDateTime(WriterContext ctx, LocalDateTime localDateTime) {
        ctx.fireLocalDateTime(localDateTime);
    }

    default void writeDateTime(WriterContext ctx, OffsetDateTime offsetDateTime) {
        ctx.fireDateTime(offsetDateTime);
    }

    default void writeDateTime(WriterContext ctx, ZonedDateTime zonedDateTime) {
        ctx.fireDateTime(zonedDateTime);
    }

    default void writeNode(
            WriterContext ctx,
            String elementId,
            long nodeId,
            TextArray labels,
            MapValue properties,
            boolean isDeleted) {
        ctx.fireNode(elementId, nodeId, labels, properties, isDeleted);
    }

    default void writeRelationship(
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
        ctx.fireRelationship(
                elementId,
                relId,
                startNodeElementId,
                startNodeId,
                endNodeElementId,
                endNodeId,
                type,
                properties,
                isDeleted);
    }

    default void writeUnboundRelationship(
            WriterContext ctx, String elementId, long relId, String type, MapValue properties) {
        ctx.fireUnboundRelationship(elementId, relId, type, properties);
    }

    default void writePath(WriterContext ctx, NodeValue[] nodes, RelationshipValue[] relationships) {
        ctx.firePath(nodes, relationships);
    }
}
