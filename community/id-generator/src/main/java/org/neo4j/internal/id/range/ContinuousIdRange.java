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
package org.neo4j.internal.id.range;

import org.neo4j.internal.id.IdGenerator;

/**
 * Range that covers ids for the whole page of particular store
 */
public class ContinuousIdRange implements PageIdRange {
    private final long rangeStart;
    private final int rangeSize;
    private final long idsPerPage;
    private int cursor = 0;

    public ContinuousIdRange(long rangeStart, int rangeSize, long idsPerPage) {
        this.rangeStart = rangeStart;
        this.rangeSize = rangeSize;
        this.idsPerPage = idsPerPage;
    }

    @Override
    public long nextId() {
        return rangeStart + (cursor++);
    }

    @Override
    public boolean hasNext() {
        return cursor < rangeSize;
    }

    @Override
    public void unallocate(IdGenerator.TransactionalMarker marker) {
        if (hasNext()) {
            long firstIdToRelease = rangeStart + cursor;
            int numberOfIds = rangeSize - cursor;
            marker.markUnallocated(firstIdToRelease, numberOfIds);
        }
    }

    @Override
    public long pageId() {
        return rangeStart / idsPerPage;
    }

    public int getRangeSize() {
        return rangeSize;
    }

    @Override
    public String toString() {
        return "ContinuousIdRange{" + "rangeStart="
                + rangeStart + ", rangeSize="
                + rangeSize + ", cursor="
                + cursor + '}';
    }
}
