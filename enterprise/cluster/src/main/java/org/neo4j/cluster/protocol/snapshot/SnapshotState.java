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

package org.neo4j.cluster.protocol.snapshot;

import java.net.URI;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
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
                                           MessageProcessor outgoing
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
                            URI coordinator = context.getClusterContext().getConfiguration().getMembers().get( 0 );
                            outgoing.process( Message.to( SnapshotMessage.sendSnapshot, coordinator ) );
                            return refreshing;
                        }

                        case join:
                        {
                            if ( context.getClusterContext().isMe( context.getClusterContext().getConfiguration()
                                    .getMembers().get( 0 ) ) || context.getSnapshotProvider() == null )
                            {
                                return ready;
                            }
                            else
                            {
                                URI coordinator = context.getClusterContext().getConfiguration().getMembers().get( 0 );
                                outgoing.process( Message.to( SnapshotMessage.sendSnapshot, coordinator ) );
                                return refreshing;
                            }
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
                                           MessageProcessor outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case snapshot:
                        {
                            SnapshotMessage.SnapshotState state = message.getPayload();
                            state.setState( context.getSnapshotProvider() );
                            context.getLearnerContext().setLastDeliveredInstanceId( state.getLastDeliveredInstanceId() );
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
                                           MessageProcessor outgoing
                )
                        throws Throwable
                {
                    switch ( message.getMessageType() )
                    {
                        case sendSnapshot:
                        {
                            outgoing.process( Message.respond( SnapshotMessage.snapshot, message, new SnapshotMessage.SnapshotState( context.getLearnerContext().getLastDeliveredInstanceId(), context.getSnapshotProvider() ) ) );
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
