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

class RelationshipScanCursor extends RelationshipCursor implements org.neo4j.internal.kernel.api.RelationshipScanCursor
{
    private int label;
    private long next;
    private long highMark;

    RelationshipScanCursor( Read read )
    {
        super( read );
    }

    void scan( int label )
    {
        if ( getId() != NO_ID )
        {
            close();
        }
        next = 0;
        this.label = label;
        highMark = read.relationshipHighMark();
    }

    void single( long reference )
    {
        if ( getId() != NO_ID )
        {
            close();
        }
        next = reference;
        label = -1;
        highMark = NO_ID;
    }

    @Override
    public boolean next()
    {
        do
        {
            if ( next == NO_ID )
            {
                close();
                return false;
            }
            read.relationship( this, next++ );
            if ( next > highMark )
            {
                if ( highMark == NO_ID )
                {
                    next = NO_ID;
                }
                else
                {
                    highMark = read.relationshipHighMark();
                    if ( next > highMark )
                    {
                        next = NO_ID;
                    }
                }
            }
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
        setId( next = NO_ID );
    }
}
