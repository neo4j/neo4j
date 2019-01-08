/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
    String NAME = "Locking";

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
