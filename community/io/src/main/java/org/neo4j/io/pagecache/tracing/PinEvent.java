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
package org.neo4j.io.pagecache.tracing;

import org.neo4j.io.pagecache.PageSwapper;

/**
 * PinEvent describes attempt to pin a page. Possible outcomes of the pin event are:
 *  1. hit - successful pin
 *  2. successful fault - successful pin
 *  3. failed fault - failed pin
 *  4. no fault - "successfully cancelled pin", no hit, but fault not happened because PF_NO_FAULT flag set
 *
 *  Invariants (equations):
 *   - faults == successful faults + failed faults
 *   - pins == hits + faults + no faults = hits + successful faults + failed faults + no faults = unpins + failed faults + no faults
 *   - unpins == hits + successful faults = hits + faults - failed faults
 *
 *   Note: default of implementation of PageCacheTracing counts total number of faults and number of failed faults.
 *   Thus, number of successful faults is calculated from those counters using subtraction.
 *
 */
public interface PinEvent extends AutoCloseablePageCacheTracerEvent {
    /**
     * A PinEvent that does nothing other than return the PageFaultEvent.NULL.
     */
    PinEvent NULL = new PinEvent() {
        @Override
        public void setCachePageId(long cachePageId) {}

        @Override
        public PinPageFaultEvent beginPageFault(long filePageId, PageSwapper pageSwapper) {
            return PinPageFaultEvent.NULL;
        }

        @Override
        public void hit() {}

        @Override
        public void noFault() {}

        @Override
        public void close() {}

        @Override
        public void snapshotsLoaded(int oldSnapshotsLoaded) {}
    };

    /**
     * The id of the cache page that holds the file page we pinned.
     */
    void setCachePageId(long cachePageId);

    /**
     * The page we want to pin is not in memory, so being a page fault to load it in.
     * @param filePageId file page id
     * @param pageSwapper file swapper
     */
    PinPageFaultEvent beginPageFault(long filePageId, PageSwapper pageSwapper);

    /**
     * Page found and bounded.
     */
    void hit();

    /**
     * No page fault happened because PF_NO_FAULT is set
     */
    void noFault();

    /**
     * Pinning complete. All related faults and flushes are completed.
     */
    @Override
    void close();

    void snapshotsLoaded(int oldSnapshotsLoaded);
}
