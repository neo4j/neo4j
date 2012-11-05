/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

/**
 * This represents the different states that a cluster member
 * can have internally.
 */
public enum HighAvailabilityMemberState
{
    /**
     * This state is the initial state, and is also the state used when leaving the cluster.
     * <p/>
     * Here we are waiting for events that transitions this member either to becoming a master or slave.
     */
    PENDING
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, URI masterURI )
                {
                    assert context.getAvailableHaMaster() == null;
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return TO_MASTER;
                    }
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, URI masterURI, URI
                        masterHaURI )
                {
//                    assert context.getAvailableMaster() == null;
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        throw new RuntimeException( "this cannot be happening" );
                    }
                    return TO_SLAVE;
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, URI slaveUri )
                {
                    if ( slaveUri.equals( context.getMyId() ) )
                    {
                        throw new RuntimeException( "cannot go from pending to slave" );
                    }
                    return this;
                }

                @Override
                public boolean isAccessAllowed( HighAvailabilityMemberContext context )
                {
                    return false;
                }
            },

    /**
     * Member now knows that a master is available, and is transitioning itself to become a slave to that master.
     * It is performing the transition process here, and so is not yet available as a slave.
     */
    TO_SLAVE
            {
                @Override
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, URI masterURI )
                {
                    if ( masterURI.equals( context.getElectedMasterId() ) )
                    {
                        // A member joined and we all got the same event
                        return this;
                    }
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return TO_MASTER;
                    }
                    // This may mean the master changed from the time we transitioned here
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, URI masterURI,
                                                             URI masterHaURI )
                {
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        throw new RuntimeException( "i (" + context.getMyId() + ") am trying to become a slave but " +
                                "someone said i am available as master" );
                    }
                    if ( masterHaURI.equals( context.getAvailableHaMaster() ) )
                    {
                        // A member joined and we all got the same event
                        return this;
                    }
                    throw new RuntimeException( "my (" + context.getMyId() + ") current master is " + context
                            .getAvailableHaMaster() + " (elected as " + context.getElectedMasterId() + " but i got a " +
                            "masterIsAvailable event for " + masterHaURI );
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, URI slaveUri )
                {
                    if ( slaveUri.equals( context.getMyId() ) )
                    {
                        return SLAVE;
                    }
                    return this;
                }

                @Override
                public boolean isAccessAllowed( HighAvailabilityMemberContext context )
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
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, URI masterURI )
                {
                    assert context.getAvailableHaMaster() == null;
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return this;
                    }
                    return PENDING; // everything still goes
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, URI masterURI,
                                                             URI masterHaURI )
                {
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return MASTER;
                    }
                    throw new RuntimeException( "i probably missed a masterIsElected event - not really that good" );
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, URI slaveUri )
                {
                    if ( slaveUri.equals( context.getMyId() ) )
                    {
                        throw new RuntimeException( "cannot be transitioning to master and slave at the same time" );
                    }
                    return this;
                }

                @Override
                public boolean isAccessAllowed( HighAvailabilityMemberContext context )
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
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, URI masterURI )
                {
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return this;
                    }
                    // This means we (probably) were disconnected and got back in the cluster
                    // and we find out that we are not the master anymore.
                    return PENDING;
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, URI masterURI,
                                                             URI masterHaURI )
                {
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return this;
                    }
                    throw new RuntimeException( "I, " + context.getMyId() + " got a masterIsAvailable for " +
                            masterHaURI + " (id is " + masterURI + " ) while being master. That should not happen" );
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, URI slaveUri )
                {
                    if ( slaveUri.equals( context.getMyId() ) )
                    {
                        throw new RuntimeException( "cannot be master and transition to slave at the same time" );
                    }
                    return this;
                }

                @Override
                public boolean isAccessAllowed( HighAvailabilityMemberContext context )
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
                public HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, URI masterURI )
                {
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        return TO_MASTER;
                    }
                    if ( masterURI.equals( context.getElectedMasterId() ) )
                    {
                        return this;
                    }
                    else
                    {
                        return PENDING;
                    }
                }

                @Override
                public HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, URI masterURI,
                                                             URI masterHaURI )
                {
                    if ( masterURI.equals( context.getMyId() ) )
                    {
                        throw new RuntimeException( "master? i don't think so" );
                    }
                    else if ( masterHaURI.equals( context.getAvailableHaMaster() ) )
                    {
                        // this is just someone else that joined the cluster
                        return this;
                    }
                    throw new RuntimeException( "i prolly missed a masterIsElected event, we're not looking good" );
                }

                @Override
                public HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, URI slaveUri )
                {
                    return this;
                }

                @Override
                public boolean isAccessAllowed( HighAvailabilityMemberContext context )
                {
                    return true;
                }
            };

    public abstract HighAvailabilityMemberState masterIsElected( HighAvailabilityMemberContext context, URI masterUri );

    public abstract HighAvailabilityMemberState masterIsAvailable( HighAvailabilityMemberContext context, URI masterClusterUri,
                                                          URI masterHaURI );

    public abstract HighAvailabilityMemberState slaveIsAvailable( HighAvailabilityMemberContext context, URI slaveUri );

    public abstract boolean isAccessAllowed( HighAvailabilityMemberContext context );
}
