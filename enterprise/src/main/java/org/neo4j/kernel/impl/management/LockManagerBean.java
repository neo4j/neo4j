/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.kernel.impl.management;

import javax.management.NotCompliantMBeanException;

import org.neo4j.kernel.management.LockManager;

@Description( "Information about the Neo4j lock status" )
class LockManagerBean extends Neo4jMBean implements LockManager
{
    private final org.neo4j.kernel.impl.transaction.LockManager lockManager;

    LockManagerBean( String instanceId, org.neo4j.kernel.impl.transaction.LockManager lockManager )
            throws NotCompliantMBeanException
    {
        super( instanceId, LockManager.class );
        this.lockManager = lockManager;
    }

    @Description( "The number of lock sequences that would have lead to a deadlock situation that "
                  + "Neo4j has detected and adverted (by throwing DeadlockDetectedException)." )
    public long getNumberOfAdvertedDeadlocks()
    {
        return lockManager.getDetectedDeadlockCount();
    }
}
