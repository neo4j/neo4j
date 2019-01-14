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
package org.neo4j.internal.kernel.api;

/**
 * Cursor for traversing the relationship groups of a node.
 */
public interface RelationshipGroupCursor extends SuspendableCursor<RelationshipGroupCursor.Position>
{
    abstract class Position extends CursorPosition<Position>
    {
    }

    /**
     * Find the first relationship group with a label greater than or equal to the provided label.
     * <p>
     * Note that the default implementation of this method (and most likely any sane use of this method - regardless of
     * implementation) assumes that relationship groups are ordered by label.
     *
     * @param relationshipLabel the relationship label to search for.
     * @return {@code true} if a matching relationship group was found, {@code false} if all relationship groups
     * within
     * reach
     * of
     * this
     * cursor were exhausted without finding a matching relationship group.
     */
    default boolean seek( int relationshipLabel )
    {
        while ( next() )
        {
            if ( relationshipLabel < type() )
            {
                return true;
            }
        }
        return false;
    }

    int type();

    int outgoingCount();

    int incomingCount();

    int loopCount();

    default int totalCount()
    {
        return outgoingCount() + incomingCount() + loopCount();
    }

    void outgoing( RelationshipTraversalCursor cursor );

    void incoming( RelationshipTraversalCursor cursor );

    void loops( RelationshipTraversalCursor cursor );

    long outgoingReference();

    long incomingReference();

    long loopsReference();
}
