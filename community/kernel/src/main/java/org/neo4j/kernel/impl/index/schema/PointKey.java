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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.HIGH;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.LOW;
import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

import org.neo4j.gis.spatial.index.curves.SpaceFillingCurve;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

class PointKey extends NativeIndexKey<PointKey> {
    private static final long[] NO_COORDINATE = EMPTY_LONG_ARRAY;

    private final IndexSpecificSpaceFillingCurveSettings settings;

    private int crsTableId;
    private int crsCode;
    private long derivedSpaceFillingCurveValue;
    private long[] coordinate;
    private Inclusion inclusion;

    PointKey(IndexSpecificSpaceFillingCurveSettings settings) {
        this.settings = settings;
    }

    @Override
    void writeValue(int stateSlot, Value value, Inclusion inclusion) {
        // we can cast without check, because the parent (who is the only production user) calls assertValidValue before
        // calling this method
        PointValue pointValue = (PointValue) value;
        CoordinateReferenceSystem crs = pointValue.getCoordinateReferenceSystem();
        crsCode = crs.getCode();
        crsTableId = crs.getTable().getTableId();

        double[] doubleCoordinate = pointValue.coordinate();
        SpaceFillingCurve spaceFillingCurve = settings.forCrs(crsTableId, crsCode);
        derivedSpaceFillingCurveValue = spaceFillingCurve.derivedValueFor(doubleCoordinate);

        coordinate = new long[doubleCoordinate.length];
        for (int i = 0; i < doubleCoordinate.length; i++) {
            coordinate[i] = Double.doubleToLongBits(doubleCoordinate[i]);
        }

        this.inclusion = inclusion;
    }

    void writePointDerived(
            CoordinateReferenceSystem crs, long derivedSpaceFillingCurveValue, NativeIndexKey.Inclusion inclusion) {
        crsCode = crs.getCode();
        crsTableId = crs.getTable().getTableId();
        this.derivedSpaceFillingCurveValue = derivedSpaceFillingCurveValue;
        coordinate = NO_COORDINATE;
        this.inclusion = inclusion;
    }

    @Override
    void assertValidValue(int stateSlot, Value value) {
        if (!(value instanceof PointValue)) {
            throw new IllegalArgumentException("Unsupported value type: " + value);
        }
    }

    @Override
    Value[] asValues() {
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get(crsTableId, crsCode);
        double[] doubleCoordinate = new double[coordinate.length];
        for (int i = 0; i < coordinate.length; i++) {
            doubleCoordinate[i] = Double.longBitsToDouble(coordinate[i]);
        }
        return new Value[] {Values.pointValue(crs, doubleCoordinate)};
    }

    @Override
    void initValueAsLowest(int stateSlot, ValueGroup valueGroup) {
        // Since table ID is the first thing that is compared,
        // using table ID that is lower than any possible real value
        // (currently the only possible values are 0, 1, 2)
        // is the simplest way how to create the lowest key.
        crsTableId = Integer.MIN_VALUE;
        crsCode = 0;
        derivedSpaceFillingCurveValue = 0;
        coordinate = NO_COORDINATE;
        inclusion = LOW;
    }

    @Override
    void initValueAsHighest(int stateSlot, ValueGroup valueGroup) {
        // Since table ID is the first thing that is compared,
        // using table ID that is higher than any possible real value
        // (currently the only possible values are 0, 1, 2)
        // is the simplest way how to create the highest key.
        crsTableId = Integer.MAX_VALUE;
        crsCode = 0;
        derivedSpaceFillingCurveValue = 0;
        coordinate = NO_COORDINATE;
        inclusion = HIGH;
    }

    @Override
    int numberOfStateSlots() {
        return 1;
    }

    @Override
    int compareValueTo(PointKey other) {
        int tableIdComparison = Integer.compare(this.crsTableId, other.crsTableId);
        if (tableIdComparison != 0) {
            return tableIdComparison;
        }

        int codeComparison = Integer.compare(this.crsCode, other.crsCode);
        if (codeComparison != 0) {
            return codeComparison;
        }

        int derivedSpaceFillingCurveValueComparison =
                Long.compare(this.derivedSpaceFillingCurveValue, other.derivedSpaceFillingCurveValue);
        if (derivedSpaceFillingCurveValueComparison != 0) {
            return derivedSpaceFillingCurveValueComparison;
        }

        // When we construct spatial sub-queries we create keys with only derived
        // space filling curve value and no actual coordinates. That's why we need
        // to check min dimensions here. In all other cases the number of dimensions
        // for a point is always given by the crs.
        int dimensions = Math.min(this.coordinate.length, other.coordinate.length);
        for (int i = 0; i < dimensions; i++) {
            // It's ok to compare the coordinate value here without deserializing them
            // because we are only defining SOME deterministic order so that we can
            // correctly separate unique points from each other, even if they collide
            // on the space filling curve.
            int coordinateComparison = Long.compare(this.coordinate[i], other.coordinate[i]);
            if (coordinateComparison != 0) {
                return coordinateComparison;
            }
        }

        return inclusion.compareTo(other.inclusion);
    }

    void copyFrom(PointKey from) {
        setEntityId(from.getEntityId());
        setCompareId(from.getCompareId());
        this.inclusion = from.inclusion;
        this.crsTableId = from.crsTableId;
        this.crsCode = from.crsCode;
        this.derivedSpaceFillingCurveValue = from.derivedSpaceFillingCurveValue;
        this.coordinate = new long[from.coordinate.length];
        System.arraycopy(from.coordinate, 0, this.coordinate, 0, from.coordinate.length);
    }

    int size() {
        // NOTE: since this index supports only one type, the type information does not have to be stored
        int coordinatesSize = PointKeyUtil.SIZE_GEOMETRY_COORDINATE * coordinate.length;
        return ENTITY_ID_SIZE
                + PointKeyUtil.SIZE_GEOMETRY_HEADER
                + PointKeyUtil.SIZE_GEOMETRY_DERIVED_SPACE_FILLING_CURVE_VALUE
                + coordinatesSize;
    }

    void writeToCursor(PageCursor cursor) {
        cursor.putLong(getEntityId());
        writePointMetadataToCursor(cursor);
        writePointValueToCursor(cursor);
    }

    private void writePointMetadataToCursor(PageCursor cursor) {
        PointKeyUtil.writeHeader(cursor, crsTableId, crsCode, coordinate.length);
    }

    private void writePointValueToCursor(PageCursor cursor) {
        cursor.putLong(derivedSpaceFillingCurveValue);
        for (int i = 0; i < coordinate.length; i++) {
            cursor.putLong(coordinate[i]);
        }
    }

    void readFromCursor(PageCursor cursor, int size) {
        initialize(cursor.getLong());
        int dimensions = readPointMetadataFromCursor(cursor);
        coordinate = new long[dimensions];
        readPointValueFromCursor(cursor);
        inclusion = NEUTRAL;

        if (size() != size) {
            cursor.setCursorException(
                    "Failed to read PointKey, because the expected size does not correspond to the stored key");
        }
    }

    private int readPointMetadataFromCursor(PageCursor cursor) {
        int header = PointKeyUtil.readHeader(cursor);
        crsTableId = PointKeyUtil.crsTableId(header);
        crsCode = PointKeyUtil.crsCode(header);
        return PointKeyUtil.dimensions(header);
    }

    private void readPointValueFromCursor(PageCursor cursor) {
        derivedSpaceFillingCurveValue = cursor.getLong();
        for (int i = 0; i < coordinate.length; i++) {
            coordinate[i] = cursor.getLong();
        }
    }

    @Override
    public String toString() {
        return "[" + asValues()[0].toString() + "],entityId=" + getEntityId();
    }
}
