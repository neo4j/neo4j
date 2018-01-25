/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.internal.kernel.api;

class StubGroupCursor implements RelationshipGroupCursor
{
    private int offset;
    private final GroupData[] groups;

    StubGroupCursor( GroupData... groups )
    {
        this.groups = groups;
        offset = -1;
    }

    void rewind()
    {
        this.offset = -1;
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
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean isClosed()
    {
        return offset >= groups.length;
    }

    @Override
    public int relationshipLabel()
    {
        return groups[offset].type;
    }

    @Override
    public int outgoingCount()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int incomingCount()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int loopCount()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void outgoing( RelationshipTraversalCursor cursor )
    {
        ((StubRelationshipCursor)cursor).read( groups[offset].out );
    }

    @Override
    public void incoming( RelationshipTraversalCursor cursor )
    {
        ((StubRelationshipCursor)cursor).read( groups[offset].in );
    }

    @Override
    public void loops( RelationshipTraversalCursor cursor )
    {
        ((StubRelationshipCursor)cursor).read( groups[offset].loop );
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

        GroupData( int out, int in, int loop, int type )
        {
            this.out = out;
            this.in = in;
            this.loop = loop;
            this.type = type;
        }
    }
}
