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
package org.neo4j.kernel.impl.index.schema;

import static java.lang.String.format;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.index.schema.GeometryType.dimensions;
import static org.neo4j.kernel.impl.index.schema.GeometryType.putCrs;
import static org.neo4j.kernel.impl.index.schema.GeometryType.putPoint;
import static org.neo4j.kernel.impl.index.schema.GeometryType.readCrs;

import java.util.Arrays;
import java.util.StringJoiner;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointArray;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

/**
 * Handles {@link PointValue[]}.
 *
 * Note about lazy initialization of {@link RangeKey} data structures: a point type is special in that it contains a {@link CoordinateReferenceSystem},
 * which dictates how much space it will occupy. When serializing a {@link PointArray} into {@link RangeKey} (via the logic in this class)
 * the {@link CoordinateReferenceSystem} isn't known at initialization, where only the type and array length is known.
 * This is why some state is initialize lazily when observing the first point in the array.
 */
class GeometryArrayType extends AbstractArrayType<PointValue> {
    // Affected key state:
    // long0Array (coordinates)
    // long0 (coordinate reference system tableId)
    // long1 (coordinate reference system code)
    // long2 (dimensions)

    GeometryArrayType(byte typeId) {
        super(
                ValueGroup.GEOMETRY_ARRAY,
                typeId,
                (o1, o2, i) -> GeometryType.compare(
                        o1.long0,
                        o1.long1,
                        o1.long2,
                        o1.long0Array,
                        (int) o1.long2 * i,
                        o2.long0,
                        o2.long1,
                        o2.long2,
                        o2.long0Array,
                        (int) o2.long2 * i),
                null,
                null,
                null,
                null,
                null);
    }

    @Override
    int valueSize(GenericKey<?> state) {
        return PointKeyUtil.SIZE_GEOMETRY_HEADER
                + arrayKeySize(state, dimensions(state) * PointKeyUtil.SIZE_GEOMETRY_COORDINATE);
    }

    @Override
    void copyValue(GenericKey<?> to, GenericKey<?> from, int arrayLength) {
        initializeArray(to, arrayLength, null);
        to.long0 = from.long0;
        to.long1 = from.long1;
        to.long2 = from.long2;
        int dimensions = dimensions(from);
        to.long0Array = ensureBigEnough(to.long0Array, dimensions * arrayLength);
        System.arraycopy(from.long0Array, 0, to.long0Array, 0, dimensions * arrayLength);
    }

    @Override
    void initializeArray(GenericKey<?> key, int length, ValueWriter.ArrayType arrayType) {
        // Since this method is called when serializing a PointValue into the key state, the CRS and number of
        // dimensions
        // are unknown at this point. Read more about why lazy initialization is required in the class-level javadoc.
        if (length == 0 && key.long0Array == null) {
            // There's this special case where we're initializing an empty geometry array and so the long0Array
            // won't be initialized at all. Therefore we're preemptively making sure it's at least not null.
            key.long0Array = EMPTY_LONG_ARRAY;
        }
    }

    @Override
    Value asValue(GenericKey<?> state) {
        Point[] points = new Point[state.arrayLength];
        if (points.length > 0) {
            CoordinateReferenceSystem crs = CoordinateReferenceSystem.get((int) state.long0, (int) state.long1);
            int dimensions = dimensions(state);
            for (int i = 0; i < points.length; i++) {
                points[i] = GeometryType.asValue(state, crs, dimensions * i);
            }
        }
        return Values.pointArray(points);
    }

    @Override
    void putValue(PageCursor cursor, GenericKey<?> state) {
        putCrs(cursor, state.long0, state.long1, state.long2);
        int dimensions = dimensions(state);
        putArray(cursor, state, (c, k, i) -> putPoint(c, k.long2, k.long0Array, i * dimensions));
    }

    @Override
    boolean readValue(PageCursor cursor, int size, GenericKey<?> into) {
        readCrs(cursor, into);
        return readArray(cursor, ValueWriter.ArrayType.POINT, GeometryArrayType::readGeometryArrayItem, into);
    }

    @Override
    String toString(GenericKey<?> state) {
        return format(
                "GeometryArray2[tableId:%d, code:%d, value:%s]",
                state.long0, state.long1, asValue(state).toString());
    }

    private static boolean readGeometryArrayItem(PageCursor cursor, GenericKey<?> into) {
        int dimensions = dimensions(into);
        if (into.currentArrayOffset == 0) {
            // Read more about why lazy initialization is required in the class-level javadoc.
            into.long0Array = ensureBigEnough(into.long0Array, dimensions * into.arrayLength);
        }
        for (int i = 0, offset = into.currentArrayOffset * dimensions; i < dimensions; i++) {
            into.long0Array[offset + i] = cursor.getLong();
        }
        into.currentArrayOffset++;
        return true;
    }

    static void write(RangeKey state, int tableId, int code, int offset, double[] coordinates) {
        if (offset == 0) {
            // Read more about why lazy initialization is required in the class-level javadoc.
            int dimensions = coordinates.length;
            state.long0Array = ensureBigEnough(state.long0Array, dimensions * state.arrayLength);
            state.long2 = dimensions;
            state.long0 = tableId;
            state.long1 = code;
        }
        for (int i = 0, base = dimensions(state) * offset; i < coordinates.length; i++) {
            state.long0Array[base + i] = Double.doubleToLongBits(coordinates[i]);
        }
    }

    @Override
    protected void addTypeSpecificDetails(StringJoiner joiner, GenericKey<?> state) {
        joiner.add("long0=" + state.long0);
        joiner.add("long1=" + state.long1);
        joiner.add("long2=" + state.long2);
        joiner.add("long0Array=" + Arrays.toString(state.long0Array));
        super.addTypeSpecificDetails(joiner, state);
    }
}
