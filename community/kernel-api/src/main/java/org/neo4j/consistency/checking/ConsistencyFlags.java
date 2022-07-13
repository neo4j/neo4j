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
    public static final ConsistencyFlags DEFAULT = new ConsistencyFlags(true, true, true, true);

    public ConsistencyFlags skipCheckGraph() {
        return new ConsistencyFlags(false, checkIndexes, checkIndexStructure, checkCounts);
    }

    public ConsistencyFlags skipCheckIndexes() {
        return new ConsistencyFlags(checkGraph, false, checkIndexStructure, checkCounts);
    }

    public ConsistencyFlags skipCheckIndexStructure() {
        return new ConsistencyFlags(checkGraph, checkIndexes, false, checkCounts);
    }

    public ConsistencyFlags skipCheckCounts() {
        return new ConsistencyFlags(checkGraph, checkIndexes, checkIndexStructure, false);
    }
}
