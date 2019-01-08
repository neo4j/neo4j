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
package org.neo4j.kernel.impl.store;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.kernel.impl.store.NoStoreHeader.NO_STORE_HEADER;

public class NoStoreHeaderFormat implements StoreHeaderFormat<NoStoreHeader>
{
    public static final NoStoreHeaderFormat NO_STORE_HEADER_FORMAT = new NoStoreHeaderFormat();

    private NoStoreHeaderFormat()
    {
    }

    @Override
    public int numberOfReservedRecords()
    {
        return 0;
    }

    @Override
    public void writeHeader( PageCursor cursor )
    {
        throw new UnsupportedOperationException( "Should not be called" );
    }

    @Override
    public NoStoreHeader readHeader( PageCursor cursor )
    {
        return NO_STORE_HEADER;
    }
}
