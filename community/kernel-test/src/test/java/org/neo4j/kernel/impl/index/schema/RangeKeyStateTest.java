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

public class RangeKeyStateTest extends IndexKeyStateTest<RangeKey> {
    @Override
    boolean includePointTypesForComparisons() {
        return true;
    }

    @Override
    int getPointSerialisedSize(int dimensions) {
        if (dimensions == 2) {
            return 20;
        } else if (dimensions == 3) {
            return 28;
        } else {
            throw new RuntimeException("Did not expect spatial value with " + dimensions + " dimensions.");
        }
    }

    @Override
    int getArrayPointSerialisedSize(int dimensions) {
        if (dimensions == 2) {
            return 16;
        } else if (dimensions == 3) {
            return 24;
        } else {
            throw new RuntimeException("Did not expect spatial value with " + dimensions + " dimensions.");
        }
    }

    @Override
    Layout<RangeKey> newLayout(int numberOfSlots) {
        RangeLayout rangeLayout = new RangeLayout(numberOfSlots);
        return new Layout<>() {

            @Override
            public RangeKey newKey() {
                return rangeLayout.newKey();
            }

            @Override
            public void minimalSplitter(RangeKey left, RangeKey right, RangeKey into) {
                rangeLayout.minimalSplitter(left, right, into);
            }

            @Override
            public int compare(RangeKey k1, RangeKey k2) {
                return rangeLayout.compare(k1, k2);
            }
        };
    }
}
