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
package org.neo4j.kernel.ha.cluster;

import java.net.URI;

import org.neo4j.cluster.InstanceId;

/**
 * This represents the different states that a cluster member
 * can have internally.
 *
 * Since transitioning to master or slave can take significant time those states are explicitly modeled.
 *
 * Most common transitions:
 * PENDING -> TO_MASTER -> MASTER
 * PENDING -> TO_SLAVE -> SLAVE
 * MASTER/SLAVE -> PENDING
 */
public enum HighAvailabilityMemberState
{
    /**
     * This state is the initial state, and is also the state used when leaving the cluster.
     * <p>
     * Here we are waiting for events that transitions this member either to becoming a master or slave.
     */
    PENDING
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context,
                                                                    InstanceId masterId )
                {
                    assert context.getAvailableHaMaster() == null;
                    if ( masterId.equals( context.getMyId() ) && !context.isSlaveOnly() )
                    {
                        return TO_MASTER;
                    }
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context,
                                                                      InstanceId masterId, URI masterHaURI )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return PENDING_ELECTION;
                    }
                    return TO_SLAVE;
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context,
                                                                     InstanceId slaveId,
                                                                     URI slaveUri )
                {
                    if ( slaveId.equals( context.getMyId() ) )
                    {
                        return PENDING_ELECTION;
                    }
                    return this;
                }

                @Override
                public boolean isEligibleForElection()
                {
                    return true;
                }

                @Override
                public boolean isAccessAllowed()
                {
                    return false;
                }
            },

    /**
     * This is pending state hinting that an election should be triggered when entering this state.
     * Otherwise this state is exactly like pending, even delegates to its methods.
     *
     * It should only be used when an unexpected event is registered which we don't know how to handle. These
     * can occur with various race conditions, such as two master elections happening almost simultaneously and
     * a member goes to pending before receiving the call (from itself) that it is available as master. In this
     * case there will be another member acting as master, this node is in pending, and the slaves can be confused
     * as to which master is the real one. So, trigger an election to resolve the issue.
     */
    PENDING_ELECTION
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context,
                        InstanceId masterId )
                {
                    return PENDING.masterIsElected( context, masterId );
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context,
                        InstanceId masterId, URI masterHaURI )
                {
                    return PENDING.masterIsAvailable( context, masterId, masterHaURI );
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context,
                        InstanceId slaveId, URI slaveUri )
                {
                    return PENDING.slaveIsAvailable( context, slaveId, slaveUri );
                }

                @Override
                public boolean isEligibleForElection()
                {
                    return PENDING.isEligibleForElection();
                }

                @Override
                public boolean isAccessAllowed()
                {
                    return PENDING.isAccessAllowed();
                }
            },

    /**
     * Member now knows that a master is available, and is transitioning itself to become a slave to that master.
     * It is performing the transition process here, and so is not yet available as a slave.
     */
    TO_SLAVE
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context,
                                                                    InstanceId masterId )
                {
                    if ( masterId.equals( context.getElectedMasterId() ) )
                    {
                        // A member joined and we all got the same event
                        return this;
                    }
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return TO_MASTER;
                    }
                    // This may mean the master changed from the time we transitioned here
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context,
                                                                      InstanceId masterId,
                                                                      URI masterHaURI )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return PENDING_ELECTION;
                    }
                    if ( masterId.equals( context.getElectedMasterId() ) )
                    {
                        // A member joined and we all got the same event
                        return this;
                    }
                    return PENDING_ELECTION;
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context,
                                                                     InstanceId slaveId,
                                                                     URI slaveUri )
                {
                    if ( slaveId.equals( context.getMyId() ) )
                    {
                        return SLAVE;
                    }
                    return this;
                }

                @Override
                public boolean isEligibleForElection()
                {
                    return false;
                }

                @Override
                public boolean isAccessAllowed()
                {
                    return false;
                }
            },

    /**
     * The cluster member knows that it has been elected as master, and starts the transitioning process.
     */
    TO_MASTER
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context,
                                                                    InstanceId masterId )
                {
                    assert context.getAvailableHaMaster() == null;
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return this;
                    }
                    return PENDING; // everything still goes
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context,
                                                                      InstanceId masterId,
                                                                      URI masterHaURI )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return MASTER;
                    }
                    return PENDING_ELECTION;
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context,
                                                                     InstanceId slaveId,
                                                                     URI slaveUri )
                {
                    if ( slaveId.equals( context.getMyId() ) )
                    {
                        return PENDING_ELECTION;
                    }
                    return this;
                }

                @Override
                public boolean isEligibleForElection()
                {
                    return true;
                }

                @Override
                public boolean isAccessAllowed()
                {
                    return false;
                }
            },

    /**
     * Cluster member is available as master for other cluster members to use.
     */
    MASTER
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context,
                                                                    InstanceId masterId )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return this;
                    }
                    // This means we (probably) were disconnected and got back in the cluster
                    // and we find out that we are not the master anymore.
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context,
                                                                      InstanceId masterId,
                                                                      URI masterHaURI )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return this;
                    }
                    return PENDING_ELECTION;
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context,
                                                                     InstanceId slaveId,
                                                                     URI slaveUri )
                {
                    if ( slaveId.equals( context.getMyId() ) )
                    {
                        return PENDING_ELECTION;
                    }
                    return this;
                }

                @Override
                public boolean isEligibleForElection()
                {
                    return true;
                }

                @Override
                public boolean isAccessAllowed()
                {
                    return true;
                }
            },

    /**
     * Cluster member is ready as a slave
     */
    SLAVE
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context,
                                                                    InstanceId masterId )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return TO_MASTER;
                    }
                    if ( masterId.equals( context.getElectedMasterId() ) )
                    {
                        return this;
                    }
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context,
                                                                      InstanceId masterId,
                                                                      URI masterHaURI )
                {
                    if ( masterId.equals( context.getMyId() ) )
                    {
                        return PENDING_ELECTION;
                    }
                    else if ( masterId.equals( context.getElectedMasterId() ) )
                    {
                        // this is just someone else that joined the cluster
                        return this;
                    }
                    return PENDING_ELECTION;
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, InstanceId slaveId, URI slaveUri )
                {
                    return this;
                }

                @Override
                public boolean isEligibleForElection()
                {
                    return true;
                }

                @Override
                public boolean isAccessAllowed()
                {
                    return true;
                }
            };

    public abstract HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, InstanceId masterId );

    public abstract HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, InstanceId masterId,
                                                                   URI masterHaURI );

    public abstract HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, InstanceId slaveId, URI slaveUri );

    /**
     * The purpose of this is that an instance cannot vote in an election while becoming a slave,
     * as it is copying stores.
     *
     * @return whether the instance is eligible or not
     */
    public abstract boolean isEligibleForElection();

    public abstract boolean isAccessAllowed();
}
