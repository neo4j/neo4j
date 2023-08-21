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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;

class GBPTreeMetaTest {
    private static final int PAGE_SIZE = PageCache.PAGE_SIZE;
    private final PageCursor cursor = ByteArrayPageCursor.wrap(new byte[PAGE_SIZE]);

    @Test
    void mustReadWhatIsWritten() throws IOException {
        // given
        var layout = SimpleLongLayout.longLayout()
                .withIdentifier(666)
                .withMajorVersion(10)
                .withMinorVersion(100)
                .build();
        Meta written = Meta.from(PAGE_SIZE, layout, null, DefaultTreeNodeSelector.selector());
        int offset = cursor.getOffset();
        written.write(cursor);

        // when
        cursor.setOffset(offset);
        Meta read = Meta.read(cursor);

        // then
        assertEquals(written.getDataFormatIdentifier(), read.getDataFormatIdentifier());
        assertEquals(written.getDataFormatVersion(), read.getDataFormatVersion());
        assertEquals(written.getRootFormatIdentifier(), read.getRootFormatIdentifier());
        assertEquals(written.getRootFormatVersion(), read.getRootFormatVersion());
        assertEquals(written.getDataLayoutIdentifier(), read.getDataLayoutIdentifier());
        assertEquals(written.getDataLayoutMajorVersion(), read.getDataLayoutMajorVersion());
        assertEquals(written.getDataLayoutMinorVersion(), read.getDataLayoutMinorVersion());
        assertEquals(written.getRootLayoutIdentifier(), read.getRootLayoutIdentifier());
        assertEquals(written.getRootLayoutMajorVersion(), read.getRootLayoutMajorVersion());
        assertEquals(written.getRootLayoutMinorVersion(), read.getRootLayoutMinorVersion());
        assertEquals(written.getPayloadSize(), read.getPayloadSize());
        assertEquals(0, read.getRootFormatIdentifier());
        assertEquals(0, read.getRootFormatVersion());

        assertEquals(layout.identifier(), read.getDataLayoutIdentifier());
        assertEquals(layout.majorVersion(), read.getDataLayoutMajorVersion());
        assertEquals(layout.minorVersion(), read.getDataLayoutMinorVersion());
        assertEquals(0, written.getRootLayoutIdentifier());
        assertEquals(0, written.getRootLayoutMajorVersion());
        assertEquals(0, written.getRootLayoutMinorVersion());
    }

    @Test
    void shouldWriteAndReadRootAndDataLayouts() throws IOException {
        // given
        var rootLayout = SimpleLongLayout.longLayout()
                .withIdentifier(666)
                .withMajorVersion(10)
                .withMinorVersion(100)
                .build();
        var dataLayout = SimpleLongLayout.longLayout()
                .withIdentifier(777)
                .withMajorVersion(11)
                .withMinorVersion(101)
                .build();
        Meta written = Meta.from(PAGE_SIZE, dataLayout, rootLayout, DefaultTreeNodeSelector.selector());
        int offset = cursor.getOffset();
        written.write(cursor);

        // when
        cursor.setOffset(offset);
        Meta read = Meta.read(cursor);

        // then
        assertEquals(written.getDataFormatIdentifier(), read.getDataFormatIdentifier());
        assertEquals(written.getDataFormatVersion(), read.getDataFormatVersion());
        assertEquals(written.getRootFormatIdentifier(), read.getRootFormatIdentifier());
        assertEquals(written.getRootFormatVersion(), read.getRootFormatVersion());
        assertEquals(written.getDataLayoutIdentifier(), read.getDataLayoutIdentifier());
        assertEquals(written.getDataLayoutMajorVersion(), read.getDataLayoutMajorVersion());
        assertEquals(written.getDataLayoutMinorVersion(), read.getDataLayoutMinorVersion());
        assertEquals(written.getPayloadSize(), read.getPayloadSize());

        assertEquals(dataLayout.identifier(), read.getDataLayoutIdentifier());
        assertEquals(dataLayout.majorVersion(), read.getDataLayoutMajorVersion());
        assertEquals(dataLayout.minorVersion(), read.getDataLayoutMinorVersion());
        assertEquals(rootLayout.identifier(), written.getRootLayoutIdentifier());
        assertEquals(rootLayout.majorVersion(), written.getRootLayoutMajorVersion());
        assertEquals(rootLayout.minorVersion(), written.getRootLayoutMinorVersion());
    }
}
