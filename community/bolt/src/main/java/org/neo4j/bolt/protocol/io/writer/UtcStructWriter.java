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

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.protocol.io.pipeline.WriterContext;

/**
 * Provides support for UTC based encoding of date time values.
 * <p />
 * This writer implementation is present on legacy connections which negotiated support for the UTC date time
 * functionality.
 *
 * @deprecated Scheduled for removal in 6.0 - Contents will be merged with {@link DefaultStructWriter}.
 */
@Deprecated(forRemoval = true, since = "5.0")
public class UtcStructWriter implements StructWriter {
    private static final UtcStructWriter INSTANCE = new UtcStructWriter();

    protected UtcStructWriter() {}

    public static StructWriter getInstance() {
        return INSTANCE;
    }

    @Override
    public void writeDateTime(WriterContext ctx, ZonedDateTime zonedDateTime) {
        var zone = zonedDateTime.getZone();

        if (zone instanceof ZoneOffset) {
            StructType.DATE_TIME.writeHeader(ctx);

            ctx.buffer()
                    .writeInt(zonedDateTime.toEpochSecond())
                    .writeInt(zonedDateTime.getNano())
                    .writeInt(zonedDateTime.getOffset().getTotalSeconds());
            return;
        }

        StructType.DATE_TIME_ZONE_ID.writeHeader(ctx);

        ctx.buffer()
                .writeInt(zonedDateTime.toEpochSecond())
                .writeInt(zonedDateTime.getNano())
                .writeString(zone.getId());
    }

    @Override
    public void writeDateTime(WriterContext ctx, OffsetDateTime offsetDateTime) {
        StructType.DATE_TIME.writeHeader(ctx);

        ctx.buffer()
                .writeInt(offsetDateTime.toEpochSecond())
                .writeInt(offsetDateTime.getNano())
                .writeInt(offsetDateTime.getOffset().getTotalSeconds());
    }
}
