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
package org.neo4j.consistency.checking;

public record ConsistencyFlags(
        boolean checkGraph, boolean checkIndexes, boolean checkIndexStructure, boolean checkCounts) {
    public static final ConsistencyFlags NONE = new ConsistencyFlags(false, false, false, false);
    public static final ConsistencyFlags ALL = new ConsistencyFlags(true, true, true, true);
    public static final ConsistencyFlags DEFAULT = ALL;

    public ConsistencyFlags withCheckGraph() {
        return new ConsistencyFlags(true, checkIndexes, checkIndexStructure, checkCounts);
    }

    public ConsistencyFlags withoutCheckGraph() {
        return new ConsistencyFlags(false, checkIndexes, checkIndexStructure, checkCounts);
    }

    public ConsistencyFlags withCheckIndexes() {
        return new ConsistencyFlags(checkGraph, true, checkIndexStructure, checkCounts);
    }

    public ConsistencyFlags withoutCheckIndexes() {
        return new ConsistencyFlags(checkGraph, false, checkIndexStructure, checkCounts);
    }

    public ConsistencyFlags withCheckIndexStructure() {
        return new ConsistencyFlags(checkGraph, checkIndexes, true, checkCounts);
    }

    public ConsistencyFlags withoutCheckIndexStructure() {
        return new ConsistencyFlags(checkGraph, checkIndexes, false, checkCounts);
    }

    public ConsistencyFlags withCheckCounts() {
        return new ConsistencyFlags(checkGraph, checkIndexes, checkIndexStructure, true);
    }

    public ConsistencyFlags withoutCheckCounts() {
        return new ConsistencyFlags(checkGraph, checkIndexes, checkIndexStructure, false);
    }
}
