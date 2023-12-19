/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
            // We assume the HEADER_FROM header always exists.
            String from =  message.getHeader( Message.HEADER_FROM );
            if ( !from.equals( message.getHeader( Message.HEADER_TO ) )  )
            {
                InstanceId theId;
                if ( message.hasHeader( Message.HEADER_INSTANCE_ID ) )
                {
                    // HEADER_INSTANCE_ID is there since after 1.9.6
                    theId = new InstanceId( Integer.parseInt( message.getHeader( Message.HEADER_INSTANCE_ID ) ) );
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
                            Message.HEADER_FROM, Message.HEADER_INSTANCE_ID
                    );
                    output.offer( heartbeatMessage );
                }
            }
        }
        return true;
    }
}
