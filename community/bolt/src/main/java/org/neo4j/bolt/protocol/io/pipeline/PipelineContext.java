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
package org.neo4j.bolt.protocol.io.pipeline;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public interface PipelineContext {

    Connection connection();

    PackstreamBuf buffer();

    void writeValue(AnyValue value);

    void writePoint(CoordinateReferenceSystem crs, double[] coords);

    void writeDuration(long months, long days, long seconds, int nanos);

    void writeDate(LocalDate localDate);

    void writeTime(LocalTime localTime);

    void writeTime(OffsetTime offsetTime);

    void writeLocalDateTime(LocalDateTime localDateTime);

    void writeDateTime(OffsetDateTime offsetDateTime);

    void writeDateTime(ZonedDateTime zonedDateTime);

    void writeNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted);

    void writeRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted);

    void writeUnboundRelationship(String elementId, long relId, String type, MapValue properties);

    void writePath(NodeValue[] nodes, RelationshipValue[] relationships);
}
