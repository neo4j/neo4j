/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import static java.lang.String.format;

import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * A key instance which can handle all types of single values, i.e. not composite keys, but all value types.
 * See {@link CompositeBtreeKey} for implementation which supports composite keys.
 *
 * BtreeKey supports all types BTREE index type supports and should only be used for BTREE indexes.
 * This will be removed in 5.0 when BTREE type is removed.
 */
public class BtreeKey extends GenericKey<BtreeKey> {
    // Immutable
    private final IndexSpecificSpaceFillingCurveSettings settings;

    BtreeKey(IndexSpecificSpaceFillingCurveSettings settings) {
        this.settings = settings;
    }

    @Override
    public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) {
        if (!isArray) {
            setType(Types.GEOMETRY);
            updateCurve(crs.getTable().getTableId(), crs.getCode());
            GeometryType.write(this, spaceFillingCurve.derivedValueFor(coordinate), coordinate);
        } else {
            if (currentArrayOffset == 0) {
                updateCurve(crs.getTable().getTableId(), crs.getCode());
            } else if (this.long1 != crs.getTable().getTableId() || this.long2 != crs.getCode()) {
                throw new IllegalStateException(format(
                        "Tried to assign a geometry array containing different coordinate reference systems, first:%s, violating:%s at array position:%d",
                        CoordinateReferenceSystem.get((int) long1, (int) long2), crs, currentArrayOffset));
            }
            GeometryArrayType.write(
                    this, currentArrayOffset++, spaceFillingCurve.derivedValueFor(coordinate), coordinate);
        }
    }

    void writePointDerived(CoordinateReferenceSystem crs, long derivedValue, NativeIndexKey.Inclusion inclusion) {
        if (isArray) {
            throw new IllegalStateException(
                    "This method is intended to be called when querying, where one or more sub-ranges are derived "
                            + "from a queried range and each sub-range written to separate keys. "
                            + "As such it's unexpected that this key state thinks that it's holds state for an array");
        }
        setType(Types.GEOMETRY);
        updateCurve(crs.getTable().getTableId(), crs.getCode());
        GeometryType.write(this, derivedValue, NO_COORDINATES);
        this.inclusion = inclusion;
    }

    private void updateCurve(int tableId, int code) {
        if (this.long1 != tableId || this.long2 != code) {
            long1 = tableId;
            long2 = code;
            spaceFillingCurve = settings.forCrs(tableId, code);
        }
    }

    @Override
    BtreeKey stateSlot(int slot) {
        Preconditions.checkState(slot == 0, "BtreeKey only supports a single key slot");
        return this;
    }

    @Override
    Type[] getTypesById() {
        return Types.Btree.BY_ID;
    }

    @Override
    AbstractArrayType<?>[] getArrayTypes() {
        return Types.Btree.BY_ARRAY_TYPE;
    }

    @Override
    Type getLowestByValueGroup() {
        return Types.Btree.LOWEST_BY_VALUE_GROUP;
    }

    @Override
    Type getHighestByValueGroup() {
        return Types.Btree.HIGHEST_BY_VALUE_GROUP;
    }

    @Override
    Type[] getTypesByGroup() {
        return Types.Btree.BY_GROUP;
    }
}
