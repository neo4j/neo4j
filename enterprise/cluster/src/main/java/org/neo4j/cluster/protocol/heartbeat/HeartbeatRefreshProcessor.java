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

package org.neo4j.cluster.protocol.heartbeat;

import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageProcessor;
import org.neo4j.cluster.com.message.MessageType;

/**
 * When a message is sent out, reset the timeout for sending heartbeat to the TO host, since we only have to send i_am_alive if
 * nothing else is going on.
 */
public class HeartbeatRefreshProcessor
    implements MessageProcessor
{
    private MessageProcessor outgoing;

    public HeartbeatRefreshProcessor(MessageProcessor outgoing )
    {
        this.outgoing = outgoing;
    }

    @Override
    public void process( Message<? extends MessageType> message )
    {
        if (!message.isInternal() && !message.isBroadcast() && !message.getMessageType().equals( HeartbeatMessage.i_am_alive ))
        {
            try
            {
                String to = message.getHeader( Message.TO );
                if (!to.equals( message.getHeader( Message.FROM ) ))
                    outgoing.process( Message.internal( HeartbeatMessage.reset_send_heartbeat, new URI( to ) ) );
            }
            catch( URISyntaxException e )
            {
                e.printStackTrace();
            }
        }
    }
}
