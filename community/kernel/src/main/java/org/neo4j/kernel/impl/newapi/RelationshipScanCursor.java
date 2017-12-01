/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.io.pagecache.PageCursor;

class RelationshipScanCursor extends RelationshipCursor implements org.neo4j.internal.kernel.api.RelationshipScanCursor
{
    private int label;
    private long next;
    private long highMark;
    private PageCursor pageCursor;

    void scan( int label, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( 0 );
        }
        next = 0;
        this.label = label;
        highMark = read.relationshipHighMark();
        this.read = read;
    }

    void single( long reference, Read read )
    {
        if ( getId() != NO_ID )
        {
            reset();
        }
        if ( pageCursor == null )
        {
            pageCursor = read.relationshipPage( reference );
        }
        next = reference;
        label = -1;
        highMark = NO_ID;
        this.read = read;
    }

    @Override
    public boolean next()
    {
        do
        {
            if ( next == NO_ID )
            {
                reset();
                return false;
            }
            do
            {
                read.relationship( this, next++, pageCursor );
                if ( next > highMark )
                {
                    if ( highMark == NO_ID )
                    {
                        next = NO_ID;
                        return (label == -1 || label() == label) && inUse();
                    }
                    else
                    {
                        highMark = read.relationshipHighMark();
                        if ( next > highMark )
                        {
                            next = NO_ID;
                            return (label == -1 || label() == label) && inUse();
                        }
                    }
                }
            }
            while ( !inUse() );
        }
        while ( label != -1 && label() != label );
        return true;
    }

    @Override
    public boolean shouldRetry()
    {
        return false;
    }

    @Override
    public void close()
    {
        if ( pageCursor != null )
        {
            pageCursor.close();
            pageCursor = null;
        }
        read = null;
        reset();
    }

    private void reset()
    {
        setId( next = NO_ID );
    }

    @Override
    public boolean isClosed()
    {
        return pageCursor == null;
    }

    @Override
    public String toString()
    {
        if ( isClosed() )
        {
            return "RelationshipScanCursor[closed state]";
        }
        else
        {
            return "RelationshipScanCursor[id=" + getId() + ", open state with: highMark=" + highMark + ", next=" + next + ", label=" + label +
                    ", underlying record=" + super.toString() + " ]";
        }
    }
}
