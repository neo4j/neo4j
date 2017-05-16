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
package org.neo4j.impl.kernel.api;

public interface EdgeGroupCursor extends SuspendableCursor<EdgeGroupCursor.Position>
{
    abstract class Position extends CursorPosition<Position>
    {
    }

    /**
     * Find the first edge group with a label greater than or equal to the provided label.
     * <p>
     * Note that the default implementation of this method (and most likely any sane use of this method - regardless of
     * implementation) assumes that edge groups are ordered by label.
     *
     * @param edgeLabel
     *         the edge label to search for.
     * @return {@code true} if a matching edge group was found, {@code false} if all edge groups within reach of this
     * cursor were exhausted without finding a matching edge group.
     */
    default boolean seek( int edgeLabel )
    {
        while ( next() )
        {
            if ( edgeLabel < edgeLabel() )
            {
                return true;
            }
        }
        return false;
    }

    int edgeLabel();

    int outgoingCount();

    int incomingCount();

    int loopCount();

    default int totalCount()
    {
        // the outgoingCount and incomingCount both contain the loopCount, so we need to remove it once.
        return outgoingCount() + incomingCount() - loopCount();
    }

    void outgoing( EdgeTraversalCursor cursor );

    void incoming( EdgeTraversalCursor cursor );

    void loops( EdgeTraversalCursor cursor );

    long outgoingReference();

    long incomingReference();

    long loopsReference();
}
