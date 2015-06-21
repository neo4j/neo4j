/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format.busted;

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;

/**
 * Used in {@link Busted} record format where records may required multiple units, which mean writing and
 * reading may require, from one byte to the next, move to another place or cursor to read from or write to.
 * Encapsulates logic for checking for consistent reads and repositioning for next retry.
 */
interface SecondaryPageCursorControl extends AutoCloseable
{
    /**
     * In the event of a secondary page cursor was used this may return {@code true}, in which case
     * (at least) the second record unit needs to be re-read. The check whether or not the primary unit
     * needs to be retried happens as part of the outer "normal" read/write, not here.
     *
     * @return whether or not a potential second record unit needs to be retried.
     * @throws IOException on error reading/writing or switching {@link PageCursor}.
     * @see PageCursor#shouldRetry()
     */
    boolean shouldRetry() throws IOException;

    /**
     * Repositions cursor(s) before retrying operation after seeing that {@link #shouldRetry()} returned {@code true}.
     */
    void reposition();

    @Override
    void close();

    public static final SecondaryPageCursorControl NULL = new SecondaryPageCursorControl()
    {
        @Override
        public boolean shouldRetry() throws IOException
        {
            return false;
        }

        @Override
        public void reposition()
        {   // No need
        }

        @Override
        public void close()
        {   // Nothing to close
        }
    };
}
