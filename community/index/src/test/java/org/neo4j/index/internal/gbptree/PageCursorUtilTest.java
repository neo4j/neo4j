/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.PageCursorUtil._2B_MASK;
import static org.neo4j.index.internal.gbptree.PageCursorUtil._3B_MASK;
import static org.neo4j.index.internal.gbptree.PageCursorUtil._6B_MASK;

@ExtendWith( RandomExtension.class )
class PageCursorUtilTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldPutAndGet6BLongs()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; i++ )
        {
            long expected = random.nextLong() & _6B_MASK;
            cursor.setOffset( 0 );
            PageCursorUtil.put6BLong( cursor, expected );
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset( 0 );
            long read = PageCursorUtil.get6BLong( cursor );
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals( expected, read );
            assertTrue( read >= 0 );
            assertEquals( 0, read & ~_6B_MASK );
            assertEquals( 6, offsetAfterWrite );
            assertEquals( 6, offsetAfterRead );
        }
    }

    @Test
    void shouldPutAndGet3BInt()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; i++ )
        {
            int expected = random.nextInt() & _3B_MASK;
            cursor.setOffset( 0 );
            PageCursorUtil.put3BInt( cursor, expected );
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset( 0 );
            int read = PageCursorUtil.get3BInt( cursor );
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals( expected, read );
            assertTrue( read >= 0 );
            assertEquals( 0, read & ~_3B_MASK );
            assertEquals( 3, offsetAfterWrite );
            assertEquals( 3, offsetAfterRead );
        }
    }

    @Test
    void shouldPutAndGet3BIntAtOffset()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; i++ )
        {
            int expected = random.nextInt() & _3B_MASK;
            cursor.setOffset( 0 );
            PageCursorUtil.put3BInt( cursor, 1, expected );
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset( 0 );
            int read = PageCursorUtil.get3BInt( cursor, 1 );
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals( expected, read );
            assertTrue( read >= 0 );
            assertEquals( 0, read & ~_3B_MASK );
            assertEquals( 0, offsetAfterWrite );
            assertEquals( 0, offsetAfterRead );
        }
    }

    @Test
    void shouldPutAndGetUnsignedShort()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; i++ )
        {
            int expected = random.nextInt() & _2B_MASK;
            cursor.setOffset( 0 );
            PageCursorUtil.putUnsignedShort( cursor, expected );
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset( 0 );
            int read = PageCursorUtil.getUnsignedShort( cursor );
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals( expected, read );
            assertTrue( read >= 0 );
            assertEquals( 0, read & ~_2B_MASK );
            assertEquals( 2, offsetAfterWrite );
            assertEquals( 2, offsetAfterRead );
        }
    }

    @Test
    void shouldPutAndGetUnsignedShortAtOffset()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; i++ )
        {
            int expected = random.nextInt() & _2B_MASK;
            cursor.setOffset( 0 );
            PageCursorUtil.putUnsignedShort( cursor, 1, expected );
            int offsetAfterWrite = cursor.getOffset();
            cursor.setOffset( 0 );
            int read = PageCursorUtil.getUnsignedShort( cursor, 1 );
            int offsetAfterRead = cursor.getOffset();

            // THEN
            assertEquals( expected, read );
            assertTrue( read >= 0 );
            assertEquals( 0, read & ~_2B_MASK );
            assertEquals( 0, offsetAfterWrite );
            assertEquals( 0, offsetAfterRead );
        }
    }

    @Test
    void shouldFailOnInvalidValues()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; )
        {
            long expected = random.nextLong();
            if ( (expected & ~_6B_MASK) != 0 )
            {
                // OK here we have an invalid value
                cursor.setOffset( 0 );
                assertThrows( IllegalArgumentException.class, () -> PageCursorUtil.put6BLong( cursor, expected ) );
                i++;
            }
        }
    }
}
