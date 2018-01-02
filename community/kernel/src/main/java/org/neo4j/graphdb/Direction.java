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
package org.neo4j.graphdb;

/**
 * Defines relationship directions used when getting relationships from a node
 * or when creating traversers.
 * <p>
 * A relationship has a direction from a node's point of view. If a node is the
 * start node of a relationship it will be an {@link #OUTGOING} relationship
 * from that node's point of view. If a node is the end node of a relationship
 * it will be an {@link #INCOMING} relationship from that node's point of view.
 * The {@link #BOTH} direction is used when direction is of no importance, such
 * as "give me all" or "traverse all" relationships that are either
 * {@link #OUTGOING} or {@link #INCOMING}.
 */
public enum Direction
{
    /**
     * Defines outgoing relationships.
     */
    OUTGOING,
    /**
     * Defines incoming relationships.
     */
    INCOMING,
    /**
     * Defines both incoming and outgoing relationships.
     */
    BOTH;

    /**
     * Reverses the direction returning {@link #INCOMING} if this equals
     * {@link #OUTGOING}, {@link #OUTGOING} if this equals {@link #INCOMING} or
     * {@link #BOTH} if this equals {@link #BOTH}.
     * 
     * @return The reversed direction.
     */
    public Direction reverse()
    {
        switch ( this )
        {
            case OUTGOING:
                return INCOMING;
            case INCOMING:
                return OUTGOING;
            case BOTH:
                return BOTH;
            default:
                throw new IllegalStateException( "Unknown Direction "
                    + "enum: " + this );
        }
    }
}
