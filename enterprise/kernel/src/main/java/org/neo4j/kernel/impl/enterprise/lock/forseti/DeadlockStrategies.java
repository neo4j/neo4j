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
package org.neo4j.kernel.impl.enterprise.lock.forseti;

import org.neo4j.util.FeatureToggles;

public enum DeadlockStrategies implements ForsetiLockManager.DeadlockResolutionStrategy
{
    /**
     * When a deadlock occurs, the client with the fewest number of held locks is aborted. If both clients hold the same
     * number of
     * locks, the client with the lowest client id is aborted.
     * <p/>
     * This is one side of a long academic argument, where the other says to abort the one with the most locks held,
     * since it's old and monolithic and holding up
     * the line.
     */
    ABORT_YOUNG
            {
                @Override
                public boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith )
                {
                    if ( isSameClient( clientThatsAsking, clientWereDeadlockedWith ) )
                    {
                        return true;
                    }

                    int ourCount = clientThatsAsking.lockCount();
                    int otherCount = clientWereDeadlockedWith.lockCount();
                    if ( ourCount > otherCount )
                    {
                        // We hold more locks than the other client, we stay the course!
                        return false;
                    }
                    else if ( otherCount > ourCount )
                    {
                        // Other client holds more locks than us, yield to her seniority
                        return true;
                    }
                    else
                    {
                        return clientThatsAsking.id() >= clientWereDeadlockedWith
                                .id(); // >= to guard against bugs where a client thinks its deadlocked itself
                    }
                }
            },

    /**
     * When a deadlock occurs, the client with the highest number of held locks is aborted. If both clients hold the
     * same number of
     * locks, the client with the highest client id is aborted.
     */
    ABORT_OLD
            {
                @Override
                public boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith )
                {
                    if ( isSameClient( clientThatsAsking, clientWereDeadlockedWith ) )
                    {
                        return true;
                    }

                    return !ABORT_YOUNG.shouldAbort( clientThatsAsking, clientWereDeadlockedWith );
                }
            },

    /**
     * When a deadlock occurs, the client that is blocking the lowest number of other clients aborts.
     * If both clients have the same sized wait lists, the one with the lowest client id is aborted.
     */
    ABORT_SHORT_WAIT_LIST
            {
                @Override
                public boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith )
                {
                    if ( isSameClient( clientThatsAsking, clientWereDeadlockedWith ) )
                    {
                        return true;
                    }

                    int ourCount = clientThatsAsking.waitListSize();
                    int otherCount = clientWereDeadlockedWith.waitListSize();
                    if ( ourCount > otherCount )
                    {
                        // We have a larger wait list than the other client, we stay the course
                        return false;
                    }
                    else if ( otherCount > ourCount )
                    {
                        // Other client has a longer wait list, we yield
                        return true;
                    }
                    else
                    {
                        return clientThatsAsking.id() > clientWereDeadlockedWith.id();
                    }
                }
            },

    /**
     * When a deadlock occurs, the client that is blocking the highest number of other clients aborts.
     * If both clients have the same sized wait lists, the one with the highest client id is aborted.
     */
    ABORT_LONG_WAIT_LIST
            {
                @Override
                public boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith )
                {
                    if ( isSameClient( clientThatsAsking, clientWereDeadlockedWith ) )
                    {
                        return true;
                    }
                    return !ABORT_SHORT_WAIT_LIST.shouldAbort( clientThatsAsking, clientWereDeadlockedWith );
                }
            };

    @Override
    public abstract boolean shouldAbort( ForsetiClient clientThatsAsking, ForsetiClient clientWereDeadlockedWith );

    /**
     * To aid in experimental testing of strategies on different real workloads, allow toggling which strategy to use.
     */
    public static final ForsetiLockManager.DeadlockResolutionStrategy DEFAULT =
            FeatureToggles.flag( DeadlockStrategies.class, "strategy", ABORT_YOUNG );

    private static boolean isSameClient( ForsetiClient a, ForsetiClient b )
    {
        // This should never happen, but as a safety net, guard against bugs
        // where a client thinks it's deadlocked with itself.
        return a.id() == b.id();
    }
}
