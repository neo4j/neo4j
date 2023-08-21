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

import java.time.ZoneOffset;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.utils.TemporalUtil;

public final class TimeReader<CTX> implements StructReader<CTX, TimeValue> {
    private static final TimeReader<?> INSTANCE = new TimeReader<>();

    private TimeReader() {}

    @SuppressWarnings("unchecked")
    public static <CTX> TimeReader<CTX> getInstance() {
        return (TimeReader<CTX>) INSTANCE;
    }

    @Override
    public short getTag() {
        return StructType.TIME.getTag();
    }

    @Override
    public TimeValue read(CTX ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 2) {
            throw new IllegalStructSizeException(2, header.length());
        }

        var nanoOfDay = buffer.readInt();
        var offsetSeconds = buffer.readInt();

        if (offsetSeconds > Integer.MAX_VALUE || offsetSeconds < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("tz_offset_seconds", "Value is out of bounds");
        }

        return TimeValue.time(
                TemporalUtil.nanosOfDayToUTC(nanoOfDay, (int) offsetSeconds),
                ZoneOffset.ofTotalSeconds((int) offsetSeconds));
    }
}
