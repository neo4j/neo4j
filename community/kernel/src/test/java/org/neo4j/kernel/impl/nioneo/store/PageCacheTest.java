/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.standard.StandardPageCache;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class PageCacheTest
{
    @Rule
    public EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    private final File storeFile = new File( "myStore" );

    @Test
    public void shouldMapAndDoIO() throws Exception
    {
        // Given
        PageCache cache = newPageCache();

        // When
        PagedFile mappedFile = cache.map( storeFile, 1024 );

        // Then I should be able to write to the file
        PageCursor cursor = cache.newCursor();
        mappedFile.pin( cursor, PageLock.EXCLUSIVE, 4 );

        cursor.setOffset( 33 );
        byte[] expected = "Hello, cruel world".getBytes( "UTF-8" );
        cursor.putBytes( expected );
        cursor.putByte( (byte) 13 );
        cursor.putInt( 1337 );
        cursor.putLong( 7331 );

        // Then I should be able to read from the file
        cursor.setOffset( 33 );
        byte[] actual = new byte[expected.length];
        cursor.getBytes( actual );

        assertThat( actual, equalTo( expected ) );
        assertThat(cursor.getByte(), equalTo((byte)13));
        assertThat(cursor.getInt(), equalTo(1337));
        assertThat(cursor.getLong(), equalTo(7331l));
    }

    private PageCache newPageCache() throws IOException
    {
        EphemeralFileSystemAbstraction fs = fsRule.get();
        return new StandardPageCache( fs, 16, 1024 );
    }
}
