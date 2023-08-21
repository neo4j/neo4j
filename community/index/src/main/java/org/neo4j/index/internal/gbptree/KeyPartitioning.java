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
package org.neo4j.index.internal.gbptree;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;

record KeyPartitioning<KEY>(Layout<KEY, ?> layout) {
    public List<KEY> partition(
            SortedSet<KEY> keyCandidates, KEY fromInclusive, KEY toExclusive, int numberOfPartitions) {
        // the inclusivity of fromInclusive is handled by adding it directly to the List
        final var keys = keyCandidates.stream()
                .filter(key -> layout.compare(key, fromInclusive) > 0 && layout.compare(key, toExclusive) < 0)
                .toList();

        final var partitions = new ArrayList<KEY>();
        partitions.add(fromInclusive);

        final var stride = Math.max((1f + keys.size()) / numberOfPartitions, 1);
        for (int i = 0; i < numberOfPartitions - 1 && i < keys.size(); i++) {
            final var pos = (i + 1) * stride;
            final var split = keys.get(Math.round(pos) - 1);
            partitions.add(split);
        }

        partitions.add(toExclusive);
        return partitions;
    }
}
