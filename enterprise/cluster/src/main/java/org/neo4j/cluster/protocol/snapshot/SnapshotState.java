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
package org.neo4j.cluster.protocol.snapshot;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.statemachine.State;

/**
 * State machine for the snapshot API
 */
public enum SnapshotState
        implements State<SnapshotContext, SnapshotMessage>
{
    start
            {
                @Override
                public State<?, ?> handle( SnapshotContext context,
                                           Message<SnapshotMessage> message,
                                           MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case setSnapshotProvider:
                        {
                            SnapshotProvider snapshotProvider = message.getPayload();
                            context.setSnapshotProvider( snapshotProvider );
                            break;
                        }

                        case refreshSnapshot:
                        {
                            if ( context.getClusterContext().getConfiguration().getMembers().size() <= 1 ||
                                    context.getSnapshotProvider() == null )
                            {
                                // we are the only instance or there are no snapshots
                                return start;
                            }
                            else
                            {
                                InstanceId coordinator = context.getClusterContext().getConfiguration().getElected(
                                        ClusterConfiguration.COORDINATOR );
                                if ( coordinator != null )
                                {
                                    // there is a coordinator - ask from that
                                    outgoing.offer( Message.to( SnapshotMessage.sendSnapshot,
                                            context.getClusterContext().getConfiguration().getUriForId(
                                                    coordinator ) ) );
                                    return refreshing;
                                }
                                else
                                {
                                    return start;
                                }
                            }
                        }

                        case join:
                        {
                            // go to ready state, if someone needs snapshots they should ask for it explicitly.
                            return ready;
                        }
                    }
                    return this;
                }
            },

    refreshing
            {
                @Override
                public State<?, ?> handle( SnapshotContext context,
                                           Message<SnapshotMessage> message,
                                           MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case snapshot:
                        {
                            SnapshotMessage.SnapshotState state = message.getPayload();

                            // If we have already delivered everything that is rolled into this snapshot, ignore it
                            state.setState( context.getSnapshotProvider(), context.getClusterContext().getObjectInputStreamFactory() );

                            return ready;
                        }
                    }

                    return this;
                }
            },

    ready
            {
                @Override
                public State<?, ?> handle( SnapshotContext context,
                                           Message<SnapshotMessage> message,
                                           MessageHolder outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case refreshSnapshot:
                         {
                             if ( context.getClusterContext().getConfiguration().getMembers().size() <= 1 ||
                                     context.getSnapshotProvider() == null )
                             {
                                 // we are the only instance in the cluster or snapshots are not meaningful
                                 return ready;
                             }
                             else
                             {
                                 InstanceId coordinator = context.getClusterContext().getConfiguration().getElected(
                                         ClusterConfiguration.COORDINATOR );
                                 if ( coordinator != null && !coordinator.equals( context.getClusterContext().getMyId() ) )
                                 {
                                     // coordinator exists, ask for the snapshot
                                     outgoing.offer( Message.to( SnapshotMessage.sendSnapshot,
                                             context.getClusterContext().getConfiguration().getUriForId(
                                                     coordinator )  ) );
                                     return refreshing;
                                 }
                                 else
                                 {
                                     // coordinator is unknown, can't do much
                                     return ready;
                                 }
                             }
                         }

                        case sendSnapshot:
                        {
                            outgoing.offer( Message.respond( SnapshotMessage.snapshot, message,
                                    new SnapshotMessage.SnapshotState( context.getLearnerContext()
                                            .getLastDeliveredInstanceId(), context.getSnapshotProvider(),
                                            context.getClusterContext().getObjectOutputStreamFactory()) ) );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }
                    }

                    return this;
                }
            }
}
