/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.internal.kernel.api.AutoCloseablePlus;
import org.neo4j.internal.kernel.api.DefaultCloseListenable;

public class StubGroupCursor extends DefaultCloseListenable implements AutoCloseablePlus
{
    private int offset;
    private final GroupData[] groups;
    private boolean isClosed;

    public StubGroupCursor( GroupData... groups )
    {
        this.groups = groups;
        this.offset = -1;
        this.isClosed = false;
    }

    void rewind()
    {
        this.offset = -1;
        this.isClosed = false;
    }

    public boolean next()
    {
        offset++;
        return offset >= 0 && offset < groups.length;
    }

    public void close()
    {
        closeInternal();
        var listener = closeListener;
        if ( listener != null )
        {
            listener.onClosed( this );
        }
    }

    public void closeInternal()
    {
        isClosed = true;
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    public int type()
    {
        return groups[offset].type;
    }

    public int outgoingCount()
    {
        return groups[offset].countOut + groups[offset].countLoop;
    }

    public int incomingCount()
    {
        return groups[offset].countIn + groups[offset].countLoop;
    }

    public int totalCount()
    {
        return groups[offset].countOut + groups[offset].countIn + groups[offset].countLoop;
    }

    public static class GroupData
    {
        final int out;
        final int in;
        final int loop;
        final int type;
        int countIn;
        int countOut;
        int countLoop;

        public GroupData( int out, int in, int loop, int type )
        {
            this.out = out;
            this.in = in;
            this.loop = loop;
            this.type = type;
        }

        public GroupData withOutCount( int count )
        {
            this.countOut = count;
            return this;
        }

        public GroupData withInCount( int count )
        {
            this.countIn = count;
            return this;
        }

        public GroupData withLoopCount( int count )
        {
            this.countLoop = count;
            return this;
        }
    }
}
