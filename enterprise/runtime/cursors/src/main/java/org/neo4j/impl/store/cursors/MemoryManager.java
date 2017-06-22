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
package org.neo4j.impl.store.cursors;

public abstract class MemoryManager
{
    protected static void setup( ReadCursor cursor, long virtualAddress, PageManager page, long pageId, long base,
            int offset )
    {
        cursor.initializeMemoryAccess( virtualAddress, page, pageId, base, offset );
    }

    protected static void read(
            ReadCursor cursor,
            long virtualAddress,
            PageManager page,
            long pageId,
            long base,
            int offset,
            int bound )
    {
        page.assertValidOffset( pageId, base, offset, bound );
        cursor.moveToOtherPage( virtualAddress, pageId, base, offset );
        cursor.lockShared();
    }

    protected static void move( ReadCursor cursor, long virtualAddress, int offset, int bound )
    {
        PageManager page = cursor.pageMan;
        long pageId = cursor.pageId;
        long base = cursor.base;
        page.assertValidOffset( pageId, base, offset, bound );
        cursor.moveWithinPage( virtualAddress, offset );
        cursor.lockToken = page.moveLock( pageId, base, cursor.offset, cursor.lockToken, offset );
    }

    protected static void write(
            Writer writer,
            long virtualAddress,
            PageManager page,
            long pageId,
            long base,
            int offset,
            int bound )
    {
        page.assertValidOffset( pageId, base, offset, bound );
        writer.initializeMemoryAccess( virtualAddress, page, pageId, base, offset );
        writer.lockExclusive();
    }
}
