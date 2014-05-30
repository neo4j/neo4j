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
package org.neo4j.io.pagecache.impl.standard;

import java.io.IOException;

import org.neo4j.io.pagecache.PageLock;
import org.neo4j.io.pagecache.impl.common.OffsetTrackingCursor;

public class StandardPageCursor extends OffsetTrackingCursor
{
    private PageLock lockTypeHeld;

    public PinnablePage page()
    {
        return (PinnablePage)page;
    }

    /** The type of lock the cursor holds */
    public PageLock lockType()
    {
        return lockTypeHeld;
    }

    public void reset( PinnablePage page, PageLock lockTypeHeld )
    {
        this.lockTypeHeld = lockTypeHeld;
        super.reset( page );
    }

    public void assertNotInUse() throws IOException
    {
        if(lockTypeHeld != null)
        {
            throw new IOException( "The cursor is already in use, you need to unpin the cursor before using it again." );
        }
    }
}
