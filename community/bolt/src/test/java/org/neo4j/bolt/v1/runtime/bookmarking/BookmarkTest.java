/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.bookmarking;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BookmarkTest
{
    @Test
    public void shouldFormatAndParseBookmarkContainingTransactionId() throws Exception
    {
        // given
        long txId = 1234;

        // when
        Bookmark bookmark = Bookmark.fromString( new Bookmark( txId ).toString() );

        // then
        assertEquals( txId, bookmark.txId() );
    }

    @Test
    public void shouldParseAndFormatBookmarkContainingTransactionId() throws Exception
    {
        // given
        String expected = "neo4j:bookmark:v1:tx1234";

        // when
        String actual = new Bookmark( Bookmark.fromString( expected ).txId() ).toString();

        // then
        assertEquals( expected, actual );
    }

    @Test
    public void shouldFailWhenParsingBadlyFormattedBookmark() throws Exception
    {
        // given
        String bookmarkString = "neo4q:markbook:v9:xt998";

        // when
        try
        {
            Bookmark.fromString( bookmarkString );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenNoNumberFollowsThePrefix() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx";

        // when
        try
        {
            Bookmark.fromString( bookmarkString );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }

    @Test
    public void shouldFailWhenBookmarkHasExtraneousTrailingCharacters() throws Exception
    {
        // given
        String bookmarkString = "neo4j:bookmark:v1:tx1234supercalifragilisticexpialidocious";

        // when
        try
        {
            Bookmark.fromString( bookmarkString );
            fail( "should have thrown exception" );
        }
        catch ( Bookmark.BookmarkFormatException e )
        {
            // I've been expecting you, Mr Bond.
        }
    }
}
