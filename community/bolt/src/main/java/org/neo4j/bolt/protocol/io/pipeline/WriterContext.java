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
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.TextArray;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

public interface WriterContext extends PipelineContext {

    void firePoint(CoordinateReferenceSystem crs, double[] coords);

    void fireDuration(long months, long days, long seconds, int nanos);

    void fireDate(LocalDate localDate);

    void fireLocalTime(LocalTime localTime);

    void fireTime(OffsetTime offsetTime);

    void fireLocalDateTime(LocalDateTime localDateTime);

    void fireDateTime(OffsetDateTime offsetDateTime);

    void fireDateTime(ZonedDateTime zonedDateTime);

    void fireNode(String elementId, long nodeId, TextArray labels, MapValue properties, boolean isDeleted);

    void fireRelationship(
            String elementId,
            long relId,
            String startNodeElementId,
            long startNodeId,
            String endNodeElementId,
            long endNodeId,
            TextValue type,
            MapValue properties,
            boolean isDeleted);

    void fireUnboundRelationship(String elementId, long relId, String type, MapValue properties);

    void firePath(NodeValue[] nodes, RelationshipValue[] relationships);
}
