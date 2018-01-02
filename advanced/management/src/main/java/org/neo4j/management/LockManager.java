/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.management;

import java.util.List;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;
import org.neo4j.kernel.info.LockInfo;

@ManagementInterface( name = LockManager.NAME )
@Description( "Information about the Neo4j lock status" )
public interface LockManager
{
    final String NAME = "Locking";

    @Description( "The number of lock sequences that would have lead to a deadlock situation that "
                  + "Neo4j has detected and averted (by throwing DeadlockDetectedException)." )
    long getNumberOfAvertedDeadlocks();

    @Description( "Information about all locks held by Neo4j" )
    List<LockInfo> getLocks();

    @Description( "Information about contended locks (locks where at least one thread is waiting) held by Neo4j. "
                  + "The parameter is used to get locks where threads have waited for at least the specified number "
                  + "of milliseconds, a value of 0 retrieves all contended locks." )
    List<LockInfo> getContendedLocks( long minWaitTime );
}
