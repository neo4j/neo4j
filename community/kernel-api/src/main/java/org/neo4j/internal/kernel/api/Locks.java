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
 * Methods for acquiring and releasing locks.
 */
public interface Locks
{
    void acquireExclusiveNodeLock( long... ids );

    void acquireExclusiveRelationshipLock( long... ids );

    void acquireExclusiveExplicitIndexLock( long... ids );

    void acquireExclusiveLabelLock( long... ids );

    void releaseExclusiveNodeLock( long... ids );

    void releaseExclusiveRelationshipLock( long... ids );

    void releaseExclusiveExplicitIndexLock( long... ids );

    void releaseExclusiveLabelLock( long... ids );

    void acquireSharedNodeLock( long... ids );

    void acquireSharedRelationshipLock( long... ids );

    void acquireSharedExplicitIndexLock( long... ids );

    void acquireSharedLabelLock( long... ids );

    void releaseSharedNodeLock( long... ids );

    void releaseSharedRelationshipLock( long... ids );

    void releaseSharedExplicitIndexLock( long... ids );

    void releaseSharedLabelLock( long... ids );
}
