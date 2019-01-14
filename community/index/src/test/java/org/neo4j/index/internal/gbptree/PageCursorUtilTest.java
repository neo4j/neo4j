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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.test.rule.RandomRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.index.internal.gbptree.PageCursorUtil._6B_MASK;

public class PageCursorUtilTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldPutAndGet6BLongs()
    {
        // GIVEN
        PageCursor cursor = ByteArrayPageCursor.wrap( 10 );

        // WHEN
        for ( int i = 0; i < 1_000; i++ )
        {
            long expected = random.nextLong() & _6B_MASK;
            cursor.setOffset( 0 );
            PageCursorUtil.put6BLong( cursor, expected );
            cursor.setOffset( 0 );
            long read = PageCursorUtil.get6BLong( cursor );

            // THEN
            assertEquals( expected, read );
            assertTrue( read >= 0 );
            assertEquals( 0, read & ~_6B_MASK );
        }
    }

    @Test
    public void shouldFailOnInvalidValues()
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
                try
                {
                    PageCursorUtil.put6BLong( cursor, expected );
                    fail( "Should have failed" );
                }
                catch ( IllegalArgumentException e )
                {
                    // THEN good
                }
                i++;
            }
        }
    }
}
