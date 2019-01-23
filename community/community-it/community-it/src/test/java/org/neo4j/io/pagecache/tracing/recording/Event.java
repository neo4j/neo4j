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
package org.neo4j.io.pagecache.tracing.recording;

import org.neo4j.io.pagecache.PageSwapper;

public abstract class Event
{
    public final PageSwapper io;
    public final long pageId;

    public Event( PageSwapper io, long pageId )
    {
        this.io = io;
        this.pageId = pageId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        Event event = (Event) o;

        return pageId == event.pageId && !(io != null ? !io.equals( event.io ) : event.io != null);

    }

    @Override
    public int hashCode()
    {
        int result = io != null ? io.hashCode() : 0;
        result = 31 * result + (int) (pageId ^ (pageId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "%s{io=%s, pageId=%s}", getClass().getSimpleName(), io, pageId );
    }
}
