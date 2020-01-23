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
package org.neo4j.storageengine.api;

import java.util.stream.IntStream;

import org.neo4j.graphdb.Direction;

public interface Degrees
{
    int[] types();

    int degree( int type, Direction direction );

    default int outgoingDegree( int type )
    {
        return degree( type, Direction.OUTGOING );
    }

    default int incomingDegree( int type )
    {
        return degree( type, Direction.INCOMING );
    }

    default int totalDegree( int type )
    {
        return degree( type, Direction.BOTH );
    }

    default int degree( Direction direction )
    {
        switch ( direction )
        {
        case OUTGOING:
            return outgoingDegree();
        case INCOMING:
            return incomingDegree();
        case BOTH:
            return totalDegree();
        default:
            throw new UnsupportedOperationException( "Unknown direction " + direction );
        }
    }

    default int outgoingDegree()
    {
        return IntStream.of( types() ).map( this::outgoingDegree ).sum();
    }

    default int incomingDegree()
    {
        return IntStream.of( types() ).map( this::incomingDegree ).sum();
    }

    default int totalDegree()
    {
        return IntStream.of( types() ).map( this::totalDegree ).sum();
    }

    /**
     * {@link Degrees} instance with all zero degrees.
     */
    Degrees EMPTY = new Degrees()
    {
        private final int[] noTypes = new int[0];

        @Override
        public int[] types()
        {
            return noTypes;
        }

        @Override
        public int degree( int type, Direction direction )
        {
            return 0;
        }
    };
}
