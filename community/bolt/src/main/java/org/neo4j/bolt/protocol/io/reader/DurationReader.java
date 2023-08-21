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

import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.DurationValue;

public final class DurationReader<CTX> implements StructReader<CTX, DurationValue> {
    private static final DurationReader<?> INSTANCE = new DurationReader<>();

    private DurationReader() {}

    @SuppressWarnings("unchecked")
    public static <CTX> DurationReader<CTX> getInstance() {
        return (DurationReader<CTX>) INSTANCE;
    }

    @Override
    public short getTag() {
        return StructType.DURATION.getTag();
    }

    @Override
    public DurationValue read(CTX ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 4) {
            throw new IllegalStructSizeException(4, header.length());
        }

        var months = buffer.readInt();
        var days = buffer.readInt();
        var seconds = buffer.readInt();
        var nanos = buffer.readInt();

        return DurationValue.duration(months, days, seconds, nanos);
    }
}
