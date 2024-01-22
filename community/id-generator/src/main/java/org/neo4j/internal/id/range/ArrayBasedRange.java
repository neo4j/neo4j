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

import java.util.Arrays;
import org.neo4j.internal.id.IdGenerator;

/**
 * Array based id range that consist usually from reused ids (or when there was reserved id in the full page range).
 */
public class ArrayBasedRange implements PageIdRange {
    private final long[] ids;
    private final int idsPerPage;
    private int cursor = 0;
    private int marker;

    public ArrayBasedRange(long[] ids, int idsPerPage) {
        this.ids = ids;
        this.idsPerPage = idsPerPage;
    }

    @Override
    public long nextId() {
        return ids[cursor++];
    }

    @Override
    public boolean hasNext() {
        return cursor < ids.length;
    }

    @Override
    public void unallocate(IdGenerator.TransactionalMarker marker) {
        while (hasNext()) {
            marker.markUnallocated(nextId());
        }
    }

    @Override
    public void mark() {
        marker = cursor;
    }

    @Override
    public void resetToMark() {
        cursor = marker;
    }

    @Override
    public long pageId() {
        return ids[0] / idsPerPage;
    }

    @Override
    public String toString() {
        return "ArrayBasedRange{" + "ids=" + Arrays.toString(ids) + ", idsPerPage=" + idsPerPage + ", cursor=" + cursor
                + ", marker=" + marker + '}';
    }
}
