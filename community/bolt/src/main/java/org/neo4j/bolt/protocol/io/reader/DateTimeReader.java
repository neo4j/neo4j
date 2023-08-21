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
package org.neo4j.bolt.protocol.io.reader;

import static java.lang.String.format;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.DateTimeValue;

public final class DateTimeReader<CTX> implements StructReader<CTX, DateTimeValue> {
    private static final DateTimeReader<?> INSTANCE = new DateTimeReader<>();

    private DateTimeReader() {}

    @SuppressWarnings("unchecked")
    public static <CTX> DateTimeReader<CTX> getInstance() {
        return (DateTimeReader<CTX>) INSTANCE;
    }

    @Override
    public short getTag() {
        return StructType.DATE_TIME.getTag();
    }

    @Override
    public DateTimeValue read(CTX ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 3) {
            throw new IllegalStructSizeException(3, header.length());
        }

        var epochSecond = buffer.readInt();
        var nanos = buffer.readInt();
        var offsetSeconds = buffer.readInt();

        if (nanos > Integer.MAX_VALUE || nanos < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("nanoseconds", "Value is out of bounds");
        }
        if (offsetSeconds > Integer.MAX_VALUE || offsetSeconds < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("tz_offset_seconds", "Value is out of bounds");
        }

        ZoneOffset offset;
        Instant instant;

        try {
            offset = ZoneOffset.ofTotalSeconds((int) offsetSeconds);
            instant = Instant.ofEpochSecond(epochSecond, nanos);
        } catch (DateTimeException | ArithmeticException ex) {
            throw new IllegalStructArgumentException(
                    "seconds", format("Illegal epoch adjustment epoch seconds: %d+%d", epochSecond, nanos), ex);
        }

        return DateTimeValue.datetime(OffsetDateTime.ofInstant(instant, offset));
    }
}
