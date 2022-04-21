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

import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.CoordinateReferenceSystem;

/**
 * RangeKey supports all the same value types as BtreeKey, but handles point values differently.
 */
public class RangeKey extends GenericKey<RangeKey> {

    @Override
    Type[] getTypesById() {
        return Types.Range.BY_ID;
    }

    @Override
    AbstractArrayType<?>[] getArrayTypes() {
        return Types.Range.BY_ARRAY_TYPE;
    }

    @Override
    Type getLowestByValueGroup() {
        return Types.Range.LOWEST_BY_VALUE_GROUP;
    }

    @Override
    Type getHighestByValueGroup() {
        return Types.Range.HIGHEST_BY_VALUE_GROUP;
    }

    @Override
    Type[] getTypesByGroup() {
        return Types.Range.BY_GROUP;
    }

    @Override
    RangeKey stateSlot(int slot) {
        Preconditions.checkState(slot == 0, "RangeKey only supports a single key slot");
        return this;
    }

    @Override
    public void writePoint(CoordinateReferenceSystem crs, double[] coordinate) {
        if (!isArray) {
            setType(Types.GEOMETRY_2);
            GeometryType2.write(this, crs.getTable().getTableId(), crs.getCode(), coordinate);
        } else {
            if (currentArrayOffset != 0 && (this.long0 != crs.getTable().getTableId() || this.long1 != crs.getCode())) {
                throw new IllegalStateException(format(
                        "Tried to assign a geometry array containing different coordinate reference systems, first:%s, violating:%s at array position:%d",
                        CoordinateReferenceSystem.get((int) long0, (int) long1), crs, currentArrayOffset));
            }
            GeometryArrayType2.write(
                    this, crs.getTable().getTableId(), crs.getCode(), currentArrayOffset++, coordinate);
        }
    }
}
