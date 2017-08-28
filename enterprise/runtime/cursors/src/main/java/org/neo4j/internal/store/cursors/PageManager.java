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
package org.neo4j.internal.store.cursors;

public abstract class PageManager extends MemoryManager
{
    protected abstract void releasePage( long pageId, long base, int offset, long lockToken );

    protected abstract void assertValidOffset( long pageId, long base, int offset, int bound );

    protected abstract long sharedLock( long pageId, long base, int offset );

    protected abstract long exclusiveLock( long pageId, long base, int offset );

    protected abstract void releaseLock( long pageId, long base, int offset, long lockToken );

    protected abstract long refreshLockToken( long pageId, long base, int offset, long lockToken );

    protected long moveLock( long pageId, long base, int offset, long lockToken, int newOffset )
    {
        boolean exclusive = lockIsExclusive(lockToken);
        releaseLock( pageId, base, offset, lockToken );
        return exclusive ? exclusiveLock( pageId, base, newOffset ) : sharedLock( pageId, base, offset );
    }

    protected boolean lockIsExclusive( long lockToken )
    {
        return true;
    }

    protected abstract boolean initializeCursor(
            long virtualAddress,
            ReadCursor cursor );

    protected abstract boolean moveToVirtualAddress(
            long virtualAddress,
            ReadCursor cursor,
            long pageId,
            long base,
            int offset, long lockToken );
}
