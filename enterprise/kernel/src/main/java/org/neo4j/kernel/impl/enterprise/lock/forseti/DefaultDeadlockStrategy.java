/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

/**
 * The standard deadlock strategy. When a deadlock occurs, the client with the fewest number of held locks is aborted. If both clients hold the same number of
 * locks, the client with the lowest client id is aborted.
 *
 * This is one side of a long academic argument, where the other says to abort the one with the most locks held, since it's old and monolithic and holding up
 * the line.
 */
public class DefaultDeadlockStrategy implements ForsetiLockManager.DeadlockResolutionStrategy
{
    @Override
    public boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith )
    {
        int ourCount = clientThatsAsking.lockCount();
        int otherCount = clientWereDeadlockedWith.lockCount();
        if( ourCount > otherCount )
        {
            // We hold more locks than the other client, we stay the course!
            return false;
        }
        else if( otherCount > ourCount )
        {
            // Other client holds more locks than us, yield to her seniority
            return true;
        }
        else
        {
            return clientThatsAsking.id() >= clientWereDeadlockedWith.id(); // >= to guard against bugs where a client thinks its deadlocked itself
        }
    }
}
