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
import static org.neo4j.bolt.protocol.io.StructType.DATE;
import static org.neo4j.bolt.protocol.io.StructType.DURATION;
import static org.neo4j.bolt.protocol.io.StructType.LOCAL_DATE_TIME;
import static org.neo4j.bolt.protocol.io.StructType.LOCAL_TIME;
import static org.neo4j.bolt.protocol.io.StructType.NODE;
import static org.neo4j.bolt.protocol.io.StructType.PATH;
import static org.neo4j.bolt.protocol.io.StructType.POINT_2D;
import static org.neo4j.bolt.protocol.io.StructType.POINT_3D;
import static org.neo4j.bolt.protocol.io.StructType.RELATIONSHIP;
import static org.neo4j.bolt.protocol.io.StructType.TIME;
import static org.neo4j.bolt.protocol.io.StructType.UNBOUND_RELATIONSHIP;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.util.PrimitiveLongIntKeyValueArray;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

/**
 * Provides support for Bolt data types.
 * <p />
 * This writer implementation is present on all connections and acts as a catch-all for all supported Bolt types.
 */
@SuppressWarnings("removal") // TODO: 6.0 - Merge UtcStructWriter with this implementation
public final class DefaultStructWriter extends UtcStructWriter implements StructWriter {
    private static final DefaultStructWriter INSTANCE = new DefaultStructWriter();

    private DefaultStructWriter() {}

    public static StructWriter getInstance() {
        return INSTANCE;
    }

    @Override
    public void writePoint(WriterContext ctx, CoordinateReferenceSystem crs, double[] coords) {
        requireNonNull(crs, ">crs cannot be null");

        switch (coords.length) {
            case 2 -> writePoint2d(ctx, crs, coords[0], coords[1]);
            case 3 -> writePoint3d(ctx, crs, coords[0], coords[1], coords[2]);
            default -> throw new IllegalArgumentException("Point with 2D or 3D coordinate expected, " + "got crs=" + crs
                    + ", coordinate=" + Arrays.toString(coords));
        }
    }

    private void writePoint2d(WriterContext ctx, CoordinateReferenceSystem crs, double x, double y) {
        requireNonNull(crs, "crs cannot be null");

        POINT_2D.writeHeader(ctx);
        ctx.buffer().writeInt(crs.getCode()).writeFloat(x).writeFloat(y);
    }

    private void writePoint3d(WriterContext ctx, CoordinateReferenceSystem crs, double x, double y, double z) {
        requireNonNull(crs, "crs cannot be null");

        POINT_3D.writeHeader(ctx);
        ctx.buffer().writeInt(crs.getCode()).writeFloat(x).writeFloat(y).writeFloat(z);
    }

    @Override
    public void writeDuration(WriterContext ctx, long months, long days, long seconds, int nanos) {
        DURATION.writeHeader(ctx);

        ctx.buffer().writeInt(months).writeInt(days).writeInt(seconds).writeInt(nanos);
    }

    @Override
    public void writeDate(WriterContext ctx, LocalDate localDate) {
        requireNonNull(localDate, "date cannot be null");

        DATE.writeHeader(ctx);
        ctx.buffer().writeInt(localDate.toEpochDay());
    }

    @Override
    public void writeLocalTime(WriterContext ctx, LocalTime localTime) {
        requireNonNull(localTime, "time cannot be null");

        LOCAL_TIME.writeHeader(ctx);
        ctx.buffer().writeInt(localTime.toNanoOfDay());
    }

    @Override
    public void writeTime(WriterContext ctx, OffsetTime offsetTime) {
        requireNonNull(offsetTime, "date cannot be null");

        TIME.writeHeader(ctx);

        ctx.buffer()
                .writeInt(offsetTime.toLocalTime().toNanoOfDay())
                .writeInt(offsetTime.getOffset().getTotalSeconds());
    }

    @Override
    public void writeLocalDateTime(WriterContext ctx, LocalDateTime localDateTime) {
        requireNonNull(localDateTime, "dateTime cannot be null");

        LOCAL_DATE_TIME.writeHeader(ctx);

        ctx.buffer().writeInt(localDateTime.toEpochSecond(UTC)).writeInt(localDateTime.getNano());
    }

    @Override
    public void writeDateTime(WriterContext ctx, OffsetDateTime offsetDateTime) {
        StructType.DATE_TIME.writeHeader(ctx);

        ctx.buffer()
                .writeInt(offsetDateTime.toEpochSecond())
                .writeInt(offsetDateTime.getNano())
                .writeInt(offsetDateTime.getOffset().getTotalSeconds());
    }

    @Override
    public void writeDateTime(WriterContext ctx, ZonedDateTime zonedDateTime) {
        requireNonNull(zonedDateTime, "dateTime cannot be null");
        var epochSecond = zonedDateTime.toEpochSecond();

        var zone = zonedDateTime.getZone();

        if (zone instanceof ZoneOffset) {
            StructType.DATE_TIME.writeHeader(ctx);
            ctx.buffer()
                    .writeInt(epochSecond)
                    .writeInt(zonedDateTime.getNano())
                    .writeInt(zonedDateTime.getOffset().getTotalSeconds());
            return;
        }

        StructType.DATE_TIME_ZONE_ID.writeHeader(ctx);
        ctx.buffer().writeInt(epochSecond).writeInt(zonedDateTime.getNano()).writeString(zone.getId());
    }

    /**
     * Encodes a path structure to a given buffer.
     * <p>
     * Within Packstream, paths are encoded as three separate lists: Nodes, Relationships and Indices where the first
     * two simply encode the unique nodes and relationships present within the path.
     * <p>
     * The indices list alternates between the indices of the nodes and relationships within the nodes/relationships
     * lists respectively (beginning with the first relationship as the first node within the path will _ALWAYS_ be the
     * first node within th nodes list). While node indices are zero-based as usual, relationship indices are one-based
     * and signed in order to denote direction. For instance, the index {@code -1} refers to the 0th relationship with
     * its direction reversed (e.g. it traverses right-to-left instead of left-to-right).
     * <p>
     * For example:
     * <pre>
     *     (pers:Person)-[o:OWNS]->(comp:Computer)<-[m:Makes]-(ven:Vendor)
     *
     *     [pers, comp, ven]
     *     [o, m]
     *     [1, 1, -2, 2]
     * </pre>
     * <p>
     * Where the indices are interpreted as follows:
     * <ul>
     *     <li>{@code #0} (Value: {@code 1}) => Relationship at Index {@code 1 - 1} (#0) => {@code o}</li>
     *     <li>{@code #1} (Value: {@code 1}) => Node at Index #1 => {@code comp}</li>
     *     <li>{@code #2} (Value {@code -2}) => Relationship at Index {@code abs(-2) - 1} (#1) => {@code m}</li>
     *     <li>{@code #3} (Value {@code 2}) => Node at Index #2 => {@code ven}</li>
     * </ul>
     * <p>
     * <b>Important:</b> The first element within the {@code nodes} parameter is expected to be the first node within
     * the encoded path.
     *
     * @param nodes         an array of nodes.
     * @param relationships an array of relationships.
     */
    @Override
    public void writePath(WriterContext ctx, NodeValue[] nodes, RelationshipValue[] relationships)
            throws RuntimeException {
        PATH.writeHeader(ctx);

        if (nodes.length == 0) {
            throw new IllegalArgumentException("Illegal node/relationship combination: path contains no nodes");
        }

        if (relationships.length == 0) {
            ctx.buffer().writeListHeader(1);
            ctx.writeValue(nodes[0]);

            ctx.buffer().writeListHeader(0).writeListHeader(0);
            return;
        }

        var nodeIndices = new PrimitiveLongIntKeyValueArray(nodes.length);
        var relationshipIndices = new PrimitiveLongIntKeyValueArray(relationships.length);

        var reducedNodes = new ArrayList<NodeValue>();
        for (var node : nodes) {
            if (!nodeIndices.putIfAbsent(node.id(), reducedNodes.size())) {
                continue;
            }

            reducedNodes.add(node);
        }

        var reducedRelationships = new ArrayList<RelationshipValue>();
        for (var rel : relationships) {
            if (!relationshipIndices.putIfAbsent(rel.id(), reducedRelationships.size())) {
                continue;
            }

            reducedRelationships.add(rel);
        }

        ctx.buffer().writeList(reducedNodes, (b, node) -> ctx.writeValue(node));
        ctx.buffer()
                .writeList(
                        reducedRelationships,
                        (b, rel) -> ctx.writeUnboundRelationship(
                                rel.elementId(), rel.id(), rel.type().stringValue(), rel.properties()));

        ctx.buffer().writeListHeader(relationships.length * 2);
        var currentOrigin = nodes[0];
        for (var i = 0; i < reducedRelationships.size(); ++i) {
            var relationship = reducedRelationships.get(i);

            int targetIndex;
            if (currentOrigin.id() == relationship.startNodeId()) {
                ctx.buffer().writeInt(i + 1);
            } else {
                ctx.buffer().writeInt((-i) - 1);
            }

            targetIndex = nodeIndices.getOrDefault(nodes[i + 1].id(), -1);

            if (targetIndex == -1) {
                throw new IllegalArgumentException(
                        "Illegal node/relationship combination: Cannot locate target node for relationship #" + i);
            }

            ctx.buffer().writeInt(targetIndex);
            currentOrigin = reducedNodes.get(targetIndex);
        }
    }

    @Override
    public void writeNode(
            WriterContext ctx,
            String elementId,
            long nodeId,
            TextArray labels,
            MapValue properties,
            boolean isDeleted) {
        NODE.writeHeader(ctx);

        var stringLabels = StreamSupport.stream(labels.spliterator(), false)
                .map(label -> ((TextValue) label).stringValue())
                .collect(Collectors.toList());

        ctx.buffer().writeInt(nodeId).writeList(stringLabels, PackstreamBuf::writeString);

        ctx.writeValue(properties);

        ctx.buffer().writeString(elementId);
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
        RELATIONSHIP.writeHeader(ctx);

        ctx.buffer().writeInt(relId).writeInt(startNodeId).writeInt(endNodeId).writeString(type.stringValue());
        ctx.writeValue(properties);
        ctx.buffer().writeString(elementId).writeString(startNodeElementId).writeString(endNodeElementId);
    }

    @Override
    public void writeUnboundRelationship(
            WriterContext ctx, String elementId, long relId, String type, MapValue properties) {
        UNBOUND_RELATIONSHIP.writeHeader(ctx);

        ctx.buffer().writeInt(relId).writeString(type);
        ctx.writeValue(properties);
        ctx.buffer().writeString(elementId);
    }
}
