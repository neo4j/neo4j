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
package org.neo4j.cluster.com.message;

import java.io.Serializable;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Message for state machines which can be sent out to instances in the cluster as well.
 * <p>
 * These are typically produced and consumed by a {@link org.neo4j.cluster.statemachine.StateMachine}.
 */
public class Message<MESSAGETYPE extends MessageType>
        implements Serializable
{
    private static final long serialVersionUID = 7043669983188264476L;

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> to( MESSAGETYPE messageType, URI to )
    {
        return to( messageType, to, null );
    }

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> to( MESSAGETYPE messageType, URI to,
                                                                             Object payload )
    {
        return new Message<>( messageType, payload ).setHeader( HEADER_TO, to.toString() );
    }

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> respond( MESSAGETYPE messageType,
            Message<?> message, Object payload )
    {
        return message.hasHeader( Message.HEADER_FROM ) ? new Message<>( messageType, payload )
                .setHeader( HEADER_TO, message.getHeader( Message.HEADER_FROM ) ) : internal( messageType, payload );
    }

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> internal( MESSAGETYPE message )
    {
        return internal( message, null );
    }

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> internal( MESSAGETYPE message, Object payload )
    {
        return new Message<>( message, payload );
    }

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> timeout( MESSAGETYPE message,
                                                                                  Message<?> causedBy )
    {
        return timeout( message, causedBy, null );
    }

    public static <MESSAGETYPE extends MessageType> Message<MESSAGETYPE> timeout( MESSAGETYPE message,
                                                                                  Message<?> causedBy, Object payload )
    {
        Message<MESSAGETYPE> timeout = causedBy.copyHeadersTo( new Message<>( message, payload ),
                Message.HEADER_CONVERSATION_ID, Message.HEADER_CREATED_BY );
        int timeoutCount = 0;
        if ( causedBy.hasHeader( HEADER_TIMEOUT_COUNT ) )
        {
            timeoutCount = Integer.parseInt( causedBy.getHeader( HEADER_TIMEOUT_COUNT ) ) + 1;
        }
        timeout.setHeader( HEADER_TIMEOUT_COUNT, "" + timeoutCount );
        return timeout;
    }

    // Standard headers
    public static final String HEADER_CONVERSATION_ID = "conversation-id";
    public static final String HEADER_CREATED_BY = "created-by";
    public static final String HEADER_TIMEOUT_COUNT = "timeout-count";
    public static final String HEADER_FROM = "from";
    public static final String HEADER_TO = "to";
    public static final String HEADER_INSTANCE_ID = "instance-id";
    // Should be present only in configurationRequest messages. Value is a comma separated list of instance ids.
    // Added in 3.0.9.
    public static final String DISCOVERED = "discovered";

    private MESSAGETYPE messageType;
    private Object payload;
    private Map<String, String> headers = new HashMap<>();

    protected Message( MESSAGETYPE messageType, Object payload )
    {
        this.messageType = messageType;
        this.payload = payload;
    }

    public MESSAGETYPE getMessageType()
    {
        return messageType;
    }

    @SuppressWarnings( "unchecked" )
    public <T> T getPayload()
    {
        return (T) payload;
    }

    public Message<MESSAGETYPE> setHeader( String name, String value )
    {
        if ( value == null )
        {
            throw new IllegalArgumentException( String.format( "Header %s may not be set to null", name ) );
        }

        headers.put( name, value );
        return this;
    }

    public boolean hasHeader( String name )
    {
        return headers.containsKey( name );
    }

    public boolean isInternal()
    {
        return !headers.containsKey( Message.HEADER_TO );
    }

    public String getHeader( String name )
            throws IllegalArgumentException
    {
        String value = getHeader( name, null );
        if ( value == null )
        {
            throw new IllegalArgumentException( "No such header:" + name );
        }
        return value;
    }

    public String getHeader( String name, String defaultValue )
    {
        String value = headers.get( name );
        if ( value == null )
        {
            return defaultValue;
        }
        return value;
    }

    public <MSGTYPE extends MessageType> Message<MSGTYPE> copyHeadersTo( Message<MSGTYPE> message,
                                                                         String... names )
    {
        if ( names.length == 0 )
        {
            for ( Map.Entry<String, String> header : headers.entrySet() )
            {
                if ( !message.hasHeader( header.getKey() ) )
                {
                    message.setHeader( header.getKey(), header.getValue() );
                }
            }
        }
        else
        {
            for ( String name : names )
            {
                String value = headers.get( name );
                if ( value != null && !message.hasHeader( name ) )
                {
                    message.setHeader( name, value );
                }
            }
        }
        return message;
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

        Message<?> message = (Message<?>) o;

        if ( headers != null ? !headers.equals( message.headers ) : message.headers != null )
        {
            return false;
        }
        if ( messageType != null ? !messageType.equals( message.messageType ) : message.messageType != null )
        {
            return false;
        }
        return payload != null ? payload.equals( message.payload ) : message.payload == null;
    }

    @Override
    public int hashCode()
    {
        int result = messageType != null ? messageType.hashCode() : 0;
        result = 31 * result + (payload != null ? payload.hashCode() : 0);
        result = 31 * result + (headers != null ? headers.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return messageType.name() + headers + (payload instanceof String ? ": " + payload : "");
    }
}
