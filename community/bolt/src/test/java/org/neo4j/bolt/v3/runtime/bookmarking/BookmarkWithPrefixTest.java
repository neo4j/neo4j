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
package org.neo4j.bolt.v3.runtime.bookmarking;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.runtime.BoltResponseHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.values.storable.Values.stringValue;

class BookmarkWithPrefixTest
{
    @Test
    void shouldHaveTransactionId()
    {
        var bookmark = new BookmarkWithPrefix( 42 );

        assertEquals( 42, bookmark.txId() );
    }

    @Test
    void shouldNotHaveDatabaseId()
    {
        var bookmark = new BookmarkWithPrefix( 42 );

        assertNull( bookmark.databaseId() );
    }

    @Test
    void shouldAttachToMetadata()
    {
        var bookmark = new BookmarkWithPrefix( 42 );
        var responseHandler = mock( BoltResponseHandler.class );

        bookmark.attachTo( responseHandler );

        verify( responseHandler ).onMetadata( "bookmark", stringValue( "neo4j:bookmark:v1:tx42" ) );
    }

    @Test
    void shouldFormatAsString()
    {
        var bookmark = new BookmarkWithPrefix( 424242 );

        assertEquals( "neo4j:bookmark:v1:tx424242", bookmark.toString() );
    }
}
