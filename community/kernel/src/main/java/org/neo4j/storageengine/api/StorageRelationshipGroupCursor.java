/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.kernel.impl.newapi.RelationshipReferenceEncoding;

public interface StorageRelationshipGroupCursor extends AutoCloseable
{
    boolean next();

    void setCurrent( int groupReference, int firstOut, int firstIn, int firstLoop );

    @Override
    void close();

    int type();

    int outgoingCount();

    int incomingCount();

    int loopCount();

    /**
     * If the returned reference points to a chain of relationships that aren't physically filtered by direction and type then
     * a flag in this reference can be set so that external filtering will be performed as the cursor progresses.
     * See {@link RelationshipReferenceEncoding#encodeForFiltering(long)}.
     *
     * @return reference to a starting point for outgoing relationships with this type. Can be passed into {@link #init(long, long)} at a later point.
     */
    long outgoingReference();

    /**
     * If the returned reference points to a chain of relationships that aren't physically filtered by direction and type then
     * a flag in this reference can be set so that external filtering will be performed as the cursor progresses.
     * See {@link RelationshipReferenceEncoding#encodeForFiltering(long)}.
     *
     * @return reference to a starting point for outgoing relationships with this type. Can be passed into {@link #init(long, long)} at a later point.
     */
    long incomingReference();

    /**
     * If the returned reference points to a chain of relationships that aren't physically filtered by direction and type then
     * a flag in this reference can be set so that external filtering will be performed as the cursor progresses.
     * See {@link RelationshipReferenceEncoding#encodeForFiltering(long)}.
     *
     * @return reference to a starting point for outgoing relationships with this type. Can be passed into {@link #init(long, long)} at a later point.
     */
    long loopsReference();

    void release();

    long getOwningNode();

    long groupReference();

    void init( long nodeReference, long reference );
}
