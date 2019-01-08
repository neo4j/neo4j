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
package org.neo4j.kernel.impl.store.record;

import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.InvalidRecordException;

/**
 * Specifies what happens when loading records, based on inUse status.
 *
 * Roughly this is what happens for the different modes:
 * <ul>
 * <li>{@link RecordLoad#CHECK}: Load at least data to determine whether or not it's in use.
 * If in use then record is loaded into target and returns {@code true},
 * otherwise return {@code false}.</li>
 * <li>{@link RecordLoad#NORMAL}: Load at least data to determine whether or not it's in use.
 * if in use then record is loaded into target returns {@code true},
 * otherwise throws {@link InvalidRecordException}.</li>
 * <li>{@link RecordLoad#FORCE}: Loads record data into target regardless of whether or not record in use.
 * Returns whether or not record is in use.
 *
 */
public enum RecordLoad
{
    NORMAL, CHECK, FORCE;

    /**
     * Checks whether or not a record should be fully loaded from {@link PageCursor}, based on inUse status.
     */
    public final boolean shouldLoad( boolean inUse )
    {
        // FORCE mode always return true so that record data will always be loaded, even if not in use.
        // The other modes only loads records that are in use.
        return this == FORCE | inUse;
    }

    /**
     * Verifies that a record's in use status is in line with the mode, might throw {@link InvalidRecordException}.
     */
    public final boolean verify( AbstractBaseRecord record )
    {
        boolean inUse = record.inUse();
        if ( this == NORMAL & !inUse )
        {
            throw new InvalidRecordException( record + " not in use" );
        }
        return this == FORCE | inUse;
    }

    /**
     * Depending on the mode, this will - if a cursor error has been raised on the given {@link PageCursor} - either
     * throw an {@link InvalidRecordException} with the underlying {@link CursorException}, or clear the error condition
     * on the cursor.
     * @param cursor The {@link PageCursor} to be checked for errors.
     */
    public final void clearOrThrowCursorError( PageCursor cursor )
    {
        if ( this == NORMAL )
        {
            try
            {
                cursor.checkAndClearCursorException();
            }
            catch ( CursorException e )
            {
                throw new InvalidRecordException( e );
            }
        }
        else
        {
            // The CHECK and FORCE modes do not bother with reporting decoding errors...
            // ... but they must still clear them, since the page cursor may be reused to read other records
            cursor.clearCursorException();
        }
    }

    /**
     * Checks the given {@link PageCursor} to see if its out-of-bounds flag has been raised, and returns {@code true} if
     * that is the case <em>and</em> and out-of-bounds condition should be reported up the stack.
     * @param cursor The {@link PageCursor} to check the bounds flag for.
     * @return {@code true} if an out-of-bounds condition should be reported up the stack, {@code false} otherwise.
     */
    public boolean checkForOutOfBounds( PageCursor cursor )
    {
        return cursor.checkAndClearBoundsFlag() & this == NORMAL;
    }
}
