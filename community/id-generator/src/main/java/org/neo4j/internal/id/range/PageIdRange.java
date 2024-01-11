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
 * Range that represent ids covering specific page of the store. Range con cover absolutely new just allocated page,
 * or consist of ids that are reused for some older page in a store.
 * Range of ids belong to one transaction in a time, but leftovers can be reused by some other transaction in different
 * time window.
 */
public interface PageIdRange {
    PageIdRange EMPTY = new PageIdRange() {
        @Override
        public long nextId() {
            return -1;
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public void unallocate(IdGenerator.TransactionalMarker marker) {}

        @Override
        public long pageId() {
            return -1;
        }
    };

    static PageIdRange wrap(long[] ids, int idsPerPage) {
        if (ids[0] + ids.length - 1 == ids[ids.length - 1]) {
            return new ContinuousIdRange(ids[0], ids.length, idsPerPage);
        }
        return new ArrayBasedRange(ids, idsPerPage);
    }

    /**
     * Id from this reserved range. Should be used only in combination with {@link #hasNext()}
     */
    long nextId();

    /**
     * Check if there still any ids available in this range.
     */
    boolean hasNext();

    /**
     * Unallocate any ids that are still available in the range.
     */
    void unallocate(IdGenerator.TransactionalMarker marker);

    /**
     * Get id of the page that this range is covering.
     */
    long pageId();
}
