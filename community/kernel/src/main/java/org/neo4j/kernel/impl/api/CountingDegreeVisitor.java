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
package org.neo4j.kernel.impl.api;

import java.util.function.IntPredicate;

import org.neo4j.storageengine.api.Direction;

public class CountingDegreeVisitor implements DegreeVisitor, IntPredicate
{
    private final Direction direction;
    private final int relType;
    private final boolean stopOnFirstMatch;

    private int count;

    public CountingDegreeVisitor( Direction direction, boolean stopOnFirstMatch )
    {
        this( direction, -1, stopOnFirstMatch );
    }

    public CountingDegreeVisitor( Direction direction, int relType, boolean stopOnFirstMatch )
    {
        this.direction = direction;
        this.relType = relType;
        this.stopOnFirstMatch = stopOnFirstMatch;
    }

    @Override
    public boolean visitDegree( int type, long outgoing, long incoming, long loop )
    {
        switch ( direction )
        {
        case OUTGOING:
            count += outgoing + loop;
            break;
        case INCOMING:
            count += incoming + loop;
            break;
        case BOTH:
            count += outgoing + incoming + loop;
            break;
        default:
            throw new IllegalStateException( "Unknown direction: " + direction );
        }

        return !stopOnFirstMatch;
    }

    public int count()
    {
        return count;
    }

    @Override
    public boolean test( int value )
    {
        return relType == -1 || relType == value;
    }
}
