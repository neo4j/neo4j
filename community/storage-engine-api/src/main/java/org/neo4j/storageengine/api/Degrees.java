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

import org.neo4j.graphdb.Direction;

/**
 * Holds data about degrees for various combinations of relationship type and {@link Direction}, retrieved from a node.
 */
public interface Degrees
{
    /**
     * @return which relationship types this instances has degrees for.
     */
    int[] types();

    /**
     * @param type the relationship type to get degree for.
     * @param direction the {@link Direction} to get degree for. {@link Direction#OUTGOING} and {@link Direction#INCOMING} will include loops too.
     * @return the degree (i.e. number of relationships on the specific node) of the given relationship type and direction,
     * or {@code 0} if no degree of that combination was found.
     */
    int degree( int type, Direction direction );

    /**
     * @param type the relationship type to get degree for.
     * @return the outgoing degree, including loops (i.e. number of relationships on the specific node) of the given relationship type,
     * or {@code 0} if no degree of that combination was found.
     */
    default int outgoingDegree( int type )
    {
        return degree( type, Direction.OUTGOING );
    }

    /**
     * @param type the relationship type to get degree for.
     * @return the incoming degree, including loops (i.e. number of relationships on the specific node) of the given relationship type,
     * or {@code 0} if no degree of that combination was found.
     */
    default int incomingDegree( int type )
    {
        return degree( type, Direction.INCOMING );
    }

    /**
     * @param type the relationship type to get degree for.
     * @return the degree (i.e. number of relationships on the specific node) of the given relationship type in all directions,
     * or {@code 0} if no degree of that combination was found.
     */
    default int totalDegree( int type )
    {
        return degree( type, Direction.BOTH );
    }

    /**
     * @param direction the {@link Direction} to get degree for, regardless of type.
     * {@link Direction#OUTGOING} and {@link Direction#INCOMING} will include loops too.
     * @return the degree (i.e. number of relationships on the specific node) of the given relationship type and direction,
     * or {@code 0} if no degree of that combination was found.
     */
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

    /**
     * @return the outgoing degree, including loops (i.e. number of relationships on the specific node) or {@code 0} if no degree of that combination was found.
     */
    default int outgoingDegree()
    {
        int degree = 0;
        for ( int type : types() )
        {
            degree += outgoingDegree( type );
        }
        return degree;
    }

    /**
     * @return the incoming degree, including loops (i.e. number of relationships on the specific node) or {@code 0} if no degree of that combination was found.
     */
    default int incomingDegree()
    {
        int degree = 0;
        for ( int type : types() )
        {
            degree += incomingDegree( type );
        }
        return degree;
    }

    /**
     * @return the degree (i.e. number of relationships on the specific node).
     */
    default int totalDegree()
    {
        int degree = 0;
        for ( int type : types() )
        {
            degree += totalDegree( type );
        }
        return degree;
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

    interface Mutator
    {
        /**
         * Returns <code>true</code> if computation should continue otherwise
         * <code>false</code>
         */
        boolean add( int type, int outgoing, int incoming, int loop );

        /**
         * @return whether or not this mutator collects relationships split by type and direction.
         */
        boolean isSplit();
    }
}
