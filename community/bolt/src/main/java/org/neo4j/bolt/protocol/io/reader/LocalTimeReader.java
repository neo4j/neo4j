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
import org.neo4j.values.storable.LocalTimeValue;

public final class LocalTimeReader<CTX> implements StructReader<CTX, LocalTimeValue> {
    private static final LocalTimeReader<?> INSTANCE = new LocalTimeReader<>();

    private LocalTimeReader() {}

    @SuppressWarnings("unchecked")
    public static <CTX> LocalTimeReader<CTX> getInstance() {
        return (LocalTimeReader<CTX>) INSTANCE;
    }

    @Override
    public short getTag() {
        return StructType.LOCAL_TIME.getTag();
    }

    @Override
    public LocalTimeValue read(CTX ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 1) {
            throw new IllegalStructSizeException(1, header.length());
        }

        var nanoOfDay = buffer.readInt();
        return LocalTimeValue.localTime(nanoOfDay);
    }
}
