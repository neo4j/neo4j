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
package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageHolder;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.cluster.ClusterContext;

/**
 * When a message is received, create an I Am Alive message as well, since we know that the sending instance is up.
 * The exceptions to this rule are:
 * - when the message is of type "I Am Alive", since this would lead to a feedback loop of more and more "I Am Alive"
 *   messages being sent.
 * - when the message is of type "Suspicions", since these should be ignored for failed instances and generating an
 *   "I Am Alive" message for it would mark the instance as alive before ignoring its suspicions.
 */
public class HeartbeatIAmAliveProcessor implements MessageProcessor
{
    private final MessageHolder output;
    private final ClusterContext clusterContext;

    public HeartbeatIAmAliveProcessor( MessageHolder output, ClusterContext clusterContext )
    {
        this.output = output;
        this.clusterContext = clusterContext;
    }

    @Override
    public boolean process( Message<? extends MessageType> message )
    {
        if ( !message.isInternal() &&
                !message.getMessageType().equals( HeartbeatMessage.i_am_alive ) &&
                !message.getMessageType().equals( HeartbeatMessage.suspicions ) )
        {
            // We assume the FROM header always exists.
            String from =  message.getHeader( Message.FROM );
            if ( !from.equals( message.getHeader( Message.TO ) )  )
            {
                InstanceId theId;
                if ( message.hasHeader( Message.INSTANCE_ID ) )
                {
                    // INSTANCE_ID is there since after 1.9.6
                    theId = new InstanceId( Integer.parseInt( message.getHeader( Message.INSTANCE_ID ) ) );
                }
                else
                {
                    theId = clusterContext.getConfiguration().getIdForUri( URI.create( from ) );
                }

                if ( theId != null && clusterContext.getConfiguration().getMembers().containsKey( theId )
                        && !clusterContext.isMe( theId ) )
                {
                    Message<HeartbeatMessage> heartbeatMessage = message.copyHeadersTo(
                            Message.internal( HeartbeatMessage.i_am_alive,
                                    new HeartbeatMessage.IAmAliveState( theId ) ),
                            Message.FROM, Message.INSTANCE_ID
                    );
                    output.offer( heartbeatMessage );
                }
            }
        }
        return true;
    }
}
