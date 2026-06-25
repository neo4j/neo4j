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
package org.neo4j.internal.id.indexed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.VersionContext;

class LockedPagesTest {

    @Test
    void addDifferentNotLockedPage() {
        LockedPages lockedPages = new LockedPages();
        CursorContext context = cursorContext(1, 2);
        assertThat(lockedPages.add(1, context)).isTrue();
        assertThat(lockedPages.add(2, context)).isTrue();
        assertThat(lockedPages.add(3, context)).isTrue();
    }

    @Test
    void addLockedPage() {
        LockedPages lockedPages = new LockedPages();
        CursorContext context = cursorContext(3, 4);
        assertThat(lockedPages.add(1, context)).isTrue();
        assertThat(lockedPages.add(1, context)).isFalse();
    }

    @Test
    void tryAddLockedPageWithDifferentBoundaries() {
        LockedPages lockedPages = new LockedPages();
        CursorContext context = cursorContext(30, 31);
        assertThat(lockedPages.add(1, context)).isTrue();

        CursorContext olderContext = cursorContext(28, 29);
        assertThat(lockedPages.add(1, olderContext)).isFalse();
    }

    @Test
    void releaseLockedPage() {
        LockedPages lockedPages = new LockedPages();
        CursorContext context = cursorContext(30, 31);
        CursorContext releaseContext = cursorContext(30, 32);
        CursorContext postReleaseContext = cursorContext(33, 0);

        assertThat(lockedPages.add(1, context)).isTrue();

        lockedPages.remove(1, releaseContext);

        assertThat(lockedPages.add(1, context)).isFalse();
        assertThat(lockedPages.add(1, releaseContext)).isFalse();
        assertThat(lockedPages.add(1, postReleaseContext)).isTrue();
    }

    @Test
    void releaseOldEntriesOnMaintenance() {
        var accessibleLockMap = new ConcurrentHashMap<Long, Long>();
        LockedPages lockedPages = new LockedPages(accessibleLockMap);
        lockedPages.add(1, cursorContext(30, 31));
        lockedPages.add(2, cursorContext(35, 36));

        lockedPages.remove(1, cursorContext(32, 33));
        lockedPages.remove(2, cursorContext(36, 37));

        lockedPages.maintenance(() -> 5);
        assertThat(accessibleLockMap).hasSize(2);

        lockedPages.maintenance(() -> 34);
        assertThat(accessibleLockMap).hasSize(1);

        lockedPages.maintenance(() -> 38);
        assertThat(accessibleLockMap).isEmpty();
    }

    // remove

    private CursorContext cursorContext(long highestClosed, long committingTransactionId) {
        CursorContext cursorContext = mock(CursorContext.class);
        VersionContext versionContext = mock(VersionContext.class);
        when(versionContext.highestClosed()).thenReturn(highestClosed);
        when(versionContext.committingTransactionId()).thenReturn(committingTransactionId);
        when(cursorContext.getVersionContext()).thenReturn(versionContext);
        return cursorContext;
    }
}
