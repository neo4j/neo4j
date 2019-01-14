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
package org.neo4j.internal.kernel.api.helpers;

import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

class StubGroupCursor implements RelationshipGroupCursor
{
    private int offset;
    private final GroupData[] groups;
    private boolean isClosed;

    StubGroupCursor( GroupData... groups )
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

    @Override
    public Position suspend()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void resume( Position position )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean next()
    {
        offset++;
        return offset >= 0 && offset < groups.length;
    }

    @Override
    public void close()
    {
        isClosed = true;
    }

    @Override
    public boolean isClosed()
    {
        return isClosed;
    }

    @Override
    public int type()
    {
        return groups[offset].type;
    }

    @Override
    public int outgoingCount()
    {
        return groups[offset].countOut;
    }

    @Override
    public int incomingCount()
    {
        return groups[offset].countIn;
    }

    @Override
    public int loopCount()
    {
        return groups[offset].countLoop;
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        ((StubRelationshipCursor) cursor).read( groups[offset].out );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        ((StubRelationshipCursor) cursor).read( groups[offset].in );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        ((StubRelationshipCursor) cursor).read( groups[offset].loop );
    }

    @Override
    public long outgoingReference()
    {
        return groups[offset].out;
    }

    @Override
    public long incomingReference()
    {
        return groups[offset].in;
    }

    @Override
    public long loopsReference()
    {
        return groups[offset].loop;
    }

    static class GroupData
    {
        final int out;
        final int in;
        final int loop;
        final int type;
        int countIn;
        int countOut;
        int countLoop;

        GroupData( int out, int in, int loop, int type )
        {
            this.out = out;
            this.in = in;
            this.loop = loop;
            this.type = type;
        }

        GroupData withOutCount( int count )
        {
            this.countOut = count;
            return this;
        }

        GroupData withInCount( int count )
        {
            this.countIn = count;
            return this;
        }

        GroupData withLoopCount( int count )
        {
            this.countLoop = count;
            return this;
        }
    }
}
