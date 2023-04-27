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
package org.neo4j.internal.id.indexed;

import static org.neo4j.internal.id.indexed.IdRange.toPaddedBinaryString;

import org.eclipse.collections.api.factory.primitive.LongLists;

public class IllegalIdTransitionException extends IllegalStateException {
    private final long[] ids;

    public IllegalIdTransitionException(long idRangeIdx, long firstId, long lastId, long into, long from) {
        super(String.format(
                "Illegal addition ID state for range: %d (IDs %d-%d) transition%ninto: %s%nfrom: %s",
                idRangeIdx, firstId, lastId, toPaddedBinaryString(into), toPaddedBinaryString(from)));

        this.ids = findCollidingIds(firstId, into, from);
    }

    public long[] ids() {
        return ids;
    }

    private long[] findCollidingIds(long firstId, long into, long from) {
        var list = LongLists.mutable.empty();
        for (int i = 0; i < Long.SIZE; i++) {
            var mask = 1L << i;
            if ((into & mask) != 0 && (from & mask) != 0) {
                list.add(firstId + i);
            }
        }
        return list.toArray();
    }
}
