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
package org.neo4j.io.pagecache.checking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContext;

class AccessCheckingPageCacheTest {
    private PageCache pageCache;
    private PageCursor cursor;

    @BeforeEach
    void getPageCursor() throws IOException {
        PageCache mockedPageCache = mock(PageCache.class);
        PagedFile mockedPagedFile = mock(PagedFile.class);
        PageCursor mockedCursor = mock(PageCursor.class);
        when(mockedPagedFile.io(anyLong(), anyInt(), any())).thenReturn(mockedCursor);
        when(mockedPageCache.map(any(Path.class), anyInt(), any(), any(), any(), any(), any()))
                .thenReturn(mockedPagedFile);
        pageCache = new AccessCheckingPageCache(mockedPageCache);
        PagedFile file = pageCache.map(Path.of("some file"), 512, "database");
        cursor = file.io(0, PagedFile.PF_SHARED_READ_LOCK, CursorContext.NULL_CONTEXT);
    }

    @Test
    void shouldGrant_read_shouldRetry_close() throws Exception {
        // GIVEN
        cursor.getByte();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.close();
    }

    @Test
    void shouldGrant_read_shouldRetry_next() throws Exception {
        // GIVEN
        cursor.getByte(0);

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next();
    }

    @Test
    void shouldGrant_read_shouldRetry_next_with_id() throws Exception {
        // GIVEN
        cursor.getShort();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next(1);
    }

    @Test
    void shouldGrant_read_shouldRetry_read_shouldRetry_close() throws Exception {
        // GIVEN
        cursor.getShort(0);
        cursor.shouldRetry();
        cursor.getInt();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.close();
    }

    @Test
    void shouldGrant_read_shouldRetry_read_shouldRetry_next() throws Exception {
        // GIVEN
        cursor.getInt(0);
        cursor.shouldRetry();
        cursor.getLong();

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next();
    }

    @Test
    void shouldGrant_read_shouldRetry_read_shouldRetry_next_with_id() throws Exception {
        // GIVEN
        cursor.getLong(0);
        cursor.shouldRetry();
        cursor.getBytes(new byte[2]);

        // WHEN
        cursor.shouldRetry();

        // THEN
        cursor.next(1);
    }

    @Test
    void shouldFail_read_close() {
        // GIVEN
        cursor.getByte();

        AssertionError assertionError = assertThrows(AssertionError.class, () -> cursor.close());
        assertThat(assertionError.getMessage()).contains("shouldRetry");
    }

    @Test
    void shouldFail_read_next() {
        // GIVEN
        cursor.getByte(0);

        AssertionError assertionError = assertThrows(AssertionError.class, () -> cursor.next());
        assertThat(assertionError.getMessage()).contains("shouldRetry");
    }

    @Test
    void shouldFail_read_next_with_id() {
        // GIVEN
        cursor.getShort();

        AssertionError assertionError = assertThrows(AssertionError.class, () -> cursor.next(1));
        assertThat(assertionError.getMessage()).contains("shouldRetry");
    }

    @Test
    void shouldFail_read_shouldRetry_read_close() throws Exception {
        // GIVEN
        cursor.getShort(0);
        cursor.shouldRetry();
        cursor.getInt();

        AssertionError assertionError = assertThrows(AssertionError.class, () -> cursor.close());
        assertThat(assertionError.getMessage()).contains("shouldRetry");
    }

    @Test
    void shouldFail_read_shouldRetry_read_next() throws Exception {
        // GIVEN
        cursor.getInt(0);
        cursor.shouldRetry();
        cursor.getLong();

        AssertionError assertionError = assertThrows(AssertionError.class, () -> cursor.next());
        assertThat(assertionError.getMessage()).contains("shouldRetry");
    }

    @Test
    void shouldFail_read_shouldRetry_read_next_with_id() throws Exception {
        // GIVEN
        cursor.getLong(0);
        cursor.shouldRetry();
        cursor.getBytes(new byte[2]);

        AssertionError assertionError = assertThrows(AssertionError.class, () -> cursor.next(1));
        assertThat(assertionError.getMessage()).contains("shouldRetry");
    }
}
