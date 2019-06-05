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
package org.neo4j.kernel.impl.index.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.index.internal.gbptree.Layout;

import static org.neo4j.io.IOUtils.closeAll;

/**
 * Take multiple {@link BlockEntryCursor} that each by themselves provide block entries in sorted order and lazily merge join, providing a view over all
 * entries from given cursors in sorted order.
 * Merging is done by keeping the cursors in an array amd pick the next lowest among them until all are exhausted, comparing
 * {@link BlockEntryCursor#key()} (current key on each cursor).
 * Instances handed out from {@link #key()} and {@link #value()} are reused, consumer is responsible for creating copy if there is a need to cache results.
 */
public class MergingBlockEntryReader<KEY,VALUE> implements BlockEntryCursor<KEY,VALUE>
{
    // Means that a cursor needs to be advanced, i.e. its current head has already been used, or that it has no head yet
    private static final byte STATE_NEED_ADVANCE = 0;
    // Means that a cursor has been advanced and its current key() contains its current head
    private static final byte STATE_HAS = 1;
    // Means that a cursor has been exhausted and has no more entries in it
    private static final byte STATE_EXHAUSTED = 2;

    private final Layout<KEY,VALUE> layout;
    private List<Source> sources = new ArrayList<>();
    private Source lastReturned;

    MergingBlockEntryReader( Layout<KEY,VALUE> layout )
    {
        this.layout = layout;
    }

    void addSource( BlockEntryCursor<KEY,VALUE> source )
    {
        sources.add( new Source( source ) );
    }

    @Override
    public boolean next() throws IOException
    {
        // Figure out lowest among cursor heads
        KEY lowest = null;
        Source lowestSource = null;
        for ( Source source : sources )
        {
            KEY candidate = source.tryNext();
            if ( candidate != null && (lowest == null || layout.compare( candidate, lowest ) < 0) )
            {
                lowest = candidate;
                lowestSource = source;
            }
        }

        // Make state transitions so that this entry is now considered used
        if ( lowest != null )
        {
            lastReturned = lowestSource.takeHead();
            return true;
        }
        return false;
    }

    @Override
    public KEY key()
    {
        return lastReturned.cursor.key();
    }

    @Override
    public VALUE value()
    {
        return lastReturned.cursor.value();
    }

    @Override
    public void close() throws IOException
    {
        closeAll( sources );
    }

    private class Source implements Closeable
    {
        private final BlockEntryCursor<KEY,VALUE> cursor;
        private byte state;

        Source( BlockEntryCursor<KEY,VALUE> cursor )
        {
            this.cursor = cursor;
        }

        KEY tryNext() throws IOException
        {
            if ( state == STATE_NEED_ADVANCE )
            {
                if ( cursor.next() )
                {
                    state = STATE_HAS;
                    return cursor.key();
                }
                else
                {
                    state = STATE_EXHAUSTED;
                }
            }
            else if ( state == STATE_HAS )
            {
                return cursor.key();
            }
            return null;
        }

        Source takeHead()
        {
            state = STATE_NEED_ADVANCE;
            return this;
        }

        @Override
        public void close() throws IOException
        {
            cursor.close();
        }
    }
}
