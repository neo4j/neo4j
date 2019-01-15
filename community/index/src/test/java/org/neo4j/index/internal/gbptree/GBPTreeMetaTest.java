/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.index.internal.gbptree;

import org.junit.Test;

import java.io.IOException;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;

import static org.junit.Assert.assertEquals;

public class GBPTreeMetaTest
{
    private static final int PAGE_SIZE = PageCache.PAGE_SIZE;
    private final PageCursor cursor = ByteArrayPageCursor.wrap( new byte[PAGE_SIZE] );

    @Test
    public void mustReadWhatIsWritten() throws IOException
    {
        // given
        Layout layout = SimpleLongLayout.longLayout()
                .withIdentifier( 666 )
                .withMajorVersion( 10 )
                .withMinorVersion( 100 )
                .build();
        Meta written = new Meta( (byte) 1, (byte) 2, PAGE_SIZE, layout );
        int offset = cursor.getOffset();
        written.write( cursor, layout );

        // when
        cursor.setOffset( offset );
        Meta read = Meta.read( cursor, layout );

        // then
        assertEquals( written.getFormatIdentifier(), read.getFormatIdentifier() );
        assertEquals( written.getFormatVersion(), read.getFormatVersion() );
        assertEquals( written.getUnusedVersionSlot3(), read.getUnusedVersionSlot3() );
        assertEquals( written.getUnusedVersionSlot4(), read.getUnusedVersionSlot4() );
        assertEquals( written.getLayoutIdentifier(), read.getLayoutIdentifier() );
        assertEquals( written.getLayoutMajorVersion(), read.getLayoutMajorVersion() );
        assertEquals( written.getLayoutMinorVersion(), read.getLayoutMinorVersion() );
        assertEquals( written.getPageSize(), read.getPageSize() );
    }
}
