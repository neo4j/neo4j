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
                public SnapshotState handle( SnapshotContext context,
                                           Message<SnapshotMessage> message,
                                           MessageHolder outgoing
                )
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

                        default:
                            break;
                    }
                    return this;
                }
            },

    refreshing
            {
                @Override
                public SnapshotState handle( SnapshotContext context,
                                           Message<SnapshotMessage> message,
                                           MessageHolder outgoing
                )
                {
                    if ( message.getMessageType() == SnapshotMessage.snapshot )
                    {
                        SnapshotMessage.SnapshotState state = message.getPayload();
                        state.setState( context.getSnapshotProvider(),
                                context.getClusterContext().getObjectInputStreamFactory() );
                        return ready;
                    }

                    return this;
                }
            },

    ready
            {
                @Override
                public SnapshotState handle( SnapshotContext context,
                                           Message<SnapshotMessage> message,
                                           MessageHolder outgoing
                )
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
                                    new SnapshotMessage.SnapshotState(
                                            context.getLearnerContext().getLastDeliveredInstanceId(),
                                            context.getSnapshotProvider(),
                                            context.getClusterContext().getObjectOutputStreamFactory() ) ) );
                            break;
                        }

                        case leave:
                        {
                            return start;
                        }

                        default:
                            break;
                    }

                    return this;
                }
            }
}
