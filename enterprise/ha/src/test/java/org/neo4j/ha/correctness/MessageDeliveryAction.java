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
package org.neo4j.ha.correctness;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.function.Function;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.helpers.collection.Iterables;

class MessageDeliveryAction implements ClusterAction
{
    public static final Function<Message,ClusterAction> MESSAGE_TO_ACTION = MessageDeliveryAction::new;

    private final Message message;

    MessageDeliveryAction( Message message )
    {
        this.message = message;
    }

    @Override
    public Iterable<ClusterAction> perform( ClusterState state ) throws URISyntaxException
    {
        String to = message.getHeader( Message.HEADER_TO );
        return Iterables.map( MESSAGE_TO_ACTION, state.instance( to ).process( messageCopy() ) );
    }

    private Message<? extends MessageType> messageCopy() throws URISyntaxException
    {
        URI to = new URI( message.getHeader( Message.HEADER_TO ) );
        Message<MessageType> copy = Message.to( message.getMessageType(), to, message.getPayload());
        return message.copyHeadersTo( copy );
    }

    @Override
    public String toString()
    {
        return "(" + message.getHeader( Message.HEADER_FROM ) + ")-[" + message.getMessageType().name() + "]->(" +
                message.getHeader( Message.HEADER_TO ) + ")";
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
        if ( !first.getMessageType().equals( other.getMessageType() ) )
        {
            return false;
        }

        if ( !first.getHeader( Message.HEADER_FROM ).equals( other.getHeader( Message.HEADER_FROM ) ) )
        {
            return false;
        }

        if ( !first.getHeader( Message.HEADER_TO ).equals( other.getHeader( Message.HEADER_TO ) ) )
        {
            return false;
        }

        if ( first.getPayload() instanceof Message && other.getPayload() instanceof Message )
        {
            return messageEquals( (Message) first.getPayload(), (Message) other.getPayload() );
        }
        else if ( first.getPayload() == null )
        {
            if ( other.getPayload() != null )
            {
                return false;
            }
        }
        else if ( !first.getPayload().equals( other.getPayload() ) )
        {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode()
    {
        int result = message.getMessageType().hashCode();
        result = 31 * result + message.getHeader( Message.HEADER_FROM ).hashCode();
        result = 31 * result + message.getHeader( Message.HEADER_TO ).hashCode();
        return result;
    }
}
