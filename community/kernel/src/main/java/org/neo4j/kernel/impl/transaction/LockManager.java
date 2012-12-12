/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import java.util.List;

import javax.transaction.Transaction;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.info.LockInfo;
import org.neo4j.kernel.logging.Logging;

public interface LockManager
{
    void getReadLock( Object resource )
            throws DeadlockDetectedException, IllegalResourceException;

    void getReadLock( Object resource, Transaction tx )
                throws DeadlockDetectedException, IllegalResourceException;

    void getWriteLock( Object resource )
                        throws DeadlockDetectedException, IllegalResourceException;

    void getWriteLock( Object resource, Transaction tx )
                            throws DeadlockDetectedException, IllegalResourceException;

    void releaseReadLock( Object resource, Transaction tx )
                                throws LockNotFoundException, IllegalResourceException;

    void releaseWriteLock( Object resource, Transaction tx )
                                    throws LockNotFoundException, IllegalResourceException;

    long getDetectedDeadlockCount();

    void dumpLocksOnResource( Object resource, Logging logging );

    List<LockInfo> getAllLocks();

    List<LockInfo> getAwaitedLocks( long minWaitTime );

    void dumpRagStack( Logging logging );

    void dumpAllLocks( Logging logging );
}
