/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;

class FixedLinkedStubPageCursor extends StubPageCursor
{
    FixedLinkedStubPageCursor( int initialPageId, int size )
    {
        super( initialPageId, size );
    }

    @Override
    public PageCursor openLinkedCursor( long pageId )
    {
        // Since we always assume here that test data will be small enough for one page it's safe
        // to assume that all cursors will be be positioned into that one page.
        // And since stub cursors use byte buffers to store data we want to prevent data loss and keep already
        // created linked cursors
        if ( linkedCursor == null )
        {
            return super.openLinkedCursor( pageId );
        }
        return linkedCursor;
    }
}
