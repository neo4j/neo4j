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
package org.neo4j.storageengine.api;

public interface StorageRelationshipGroupCursor extends StorageCursor
{
    void setCurrent( int groupReference, int firstOut, int firstIn, int firstLoop );

    int type();

    int outgoingCount();

    int incomingCount();

    int loopCount();

    /**
     * @return reference to a starting point for outgoing relationships with this type. Can be passed into {@link #init(long, long)} at a later point.
     */
    long outgoingReference();

    /**
     * @return reference to a starting point for outgoing relationships with this type. Can be passed into {@link #init(long, long)} at a later point.
     */
    long incomingReference();

    /**
     * @return reference to a starting point for outgoing relationships with this type. Can be passed into {@link #init(long, long)} at a later point.
     */
    long loopsReference();

    long getOwningNode();

    long groupReference();

    void init( long nodeReference, long reference );
}
