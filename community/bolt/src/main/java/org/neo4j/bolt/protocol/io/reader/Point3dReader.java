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

import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.packstream.error.reader.PackstreamReaderException;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.packstream.error.struct.IllegalStructSizeException;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.packstream.struct.StructReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

public final class Point3dReader<CTX> implements StructReader<CTX, PointValue> {
    private static final Point3dReader<?> INSTANCE = new Point3dReader<>();

    private Point3dReader() {}

    @SuppressWarnings("unchecked")
    public static <CTX> Point3dReader<CTX> getInstance() {
        return (Point3dReader<CTX>) INSTANCE;
    }

    @Override
    public short getTag() {
        return StructType.POINT_3D.getTag();
    }

    @Override
    public PointValue read(CTX ctx, PackstreamBuf buffer, StructHeader header) throws PackstreamReaderException {
        if (header.length() != 4) {
            throw new IllegalStructSizeException(4, header.length());
        }

        var crsCode = buffer.readInt();
        var x = buffer.readFloat();
        var y = buffer.readFloat();
        var z = buffer.readFloat();

        if (crsCode > Integer.MAX_VALUE || crsCode < Integer.MIN_VALUE) {
            throw new IllegalStructArgumentException("crs", "crs code exceeds valid bounds");
        }

        CoordinateReferenceSystem crs;
        try {
            crs = CoordinateReferenceSystem.get((int) crsCode);
        } catch (InvalidArgumentException ex) {
            throw new IllegalStructArgumentException(
                    "crs", format("Illegal coordinate reference system: \"%s\"", crsCode), ex);
        }

        try {
            return Values.pointValue(crs, x, y, z);
        } catch (InvalidArgumentException ex) {
            throw new IllegalStructArgumentException(
                    "coords", format("Illegal CRS/coords combination (crs=%s, x=%s, y=%s, z=%s)", crs, x, y, z), ex);
        }
    }
}
