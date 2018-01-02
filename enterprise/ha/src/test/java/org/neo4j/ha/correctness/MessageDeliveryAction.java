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
package org.neo4j.ha.correctness;

import java.net.URI;
import java.net.URISyntaxException;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.function.Function;
import org.neo4j.helpers.collection.Iterables;

class MessageDeliveryAction implements ClusterAction
{
    public static final Function<Message,ClusterAction> MESSAGE_TO_ACTION = new Function<Message, ClusterAction>()
    {
        @Override
        public ClusterAction apply( Message message )
        {
            return new MessageDeliveryAction( message );
        }
    };

    private final Message message;

    public MessageDeliveryAction( Message message )
    {
        this.message = message;
    }

    @Override
    public Iterable<ClusterAction> perform( ClusterState state ) throws URISyntaxException
    {
        String to = message.getHeader( Message.TO );
        return Iterables.map( MESSAGE_TO_ACTION, state.instance( to ).process( messageCopy() ) );
    }

    private Message<? extends MessageType> messageCopy() throws URISyntaxException
    {
        URI to = new URI( message.getHeader( Message.TO ) );
        Message<MessageType> copy = Message.to( message.getMessageType(), to, message.getPayload());
        return message.copyHeadersTo( copy );
    }

    @Override
    public String toString()
    {
        return "("+message.getHeader( Message.FROM ) + ")-[" + message.getMessageType().name() + "]->(" + message.getHeader( Message.TO ) + ")";
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        return messageEquals( message, ((MessageDeliveryAction) o).message );

    }

    private boolean messageEquals( Message first, Message other )
    {
        if( !first.getMessageType().equals( other.getMessageType() ))
        {
            return false;
        }

        if( !first.getHeader( Message.FROM ).equals( other.getHeader( Message.FROM ) ))
        {
            return false;
        }

        if( !first.getHeader( Message.TO ).equals( other.getHeader( Message.TO ) ))
        {
            return false;
        }

        if(first.getPayload() instanceof Message && other.getPayload() instanceof Message)
        {
            return messageEquals((Message)first.getPayload(), (Message)other.getPayload() );
        }
        else if(first.getPayload() == null)
        {
            if(other.getPayload() != null)
            {
                return false;
            }
        }
        else if( !first.getPayload().equals( other.getPayload() ))
        {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = message.getMessageType().hashCode();
        result = 31 * result + message.getHeader( Message.FROM ).hashCode();
        result = 31 * result + message.getHeader( Message.TO ).hashCode();
        return result;
    }
}
