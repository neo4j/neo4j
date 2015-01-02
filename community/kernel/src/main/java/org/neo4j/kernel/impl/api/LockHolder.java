/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.ReleaseLocksFailedKernelException;

public interface LockHolder
{
    void acquireNodeReadLock( long nodeId );

    void acquireNodeWriteLock( long nodeId );

    void acquireRelationshipReadLock( long relationshipId );

    void acquireRelationshipWriteLock( long relationshipId );

    void acquireGraphWriteLock();

    void acquireSchemaReadLock();

    void acquireSchemaWriteLock();

    /**
     * @param propertyValue is a string for serialization purposes (HA). There can be clashes, but these are rare
     *                      enough, transient, and does not affect correctness.
     */
    void acquireIndexEntryWriteLock( int labelId, int propertyKeyId, String propertyValue );

    /**
     * @param propertyValue is a string for serialization purposes (HA). There can be clashes, but these are rare
     *                      enough, transient, and does not affect correctness.
     */
    ReleasableLock getReleasableIndexEntryReadLock( int labelId, int propertyKeyId, String propertyValue );

    /**
     * @param propertyValue is a string for serialization purposes (HA). There can be clashes, but these are rare
     *                      enough, transient, and does not affect correctness.
     */
    ReleasableLock getReleasableIndexEntryWriteLock( int labelId, int propertyKeyId, String propertyValue );

    void releaseLocks() throws ReleaseLocksFailedKernelException;
}
