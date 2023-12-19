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
package org.neo4j.cluster.protocol;

import org.mockito.ArgumentMatcher;

import java.io.Serializable;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

public class MessageArgumentMatcher<T extends MessageType> implements ArgumentMatcher<Message<T>>
{
    private URI from;
    private URI to;
    private T theMessageType;
    private Serializable payload;
    private final List<String> headers = new LinkedList<>();
    private final List<String> headerValues = new LinkedList<>();

    public MessageArgumentMatcher<T> from( URI from )
    {
        this.from = from;
        return this;
    }

    public MessageArgumentMatcher<T> to( URI to )
    {
        this.to = to;
        return this;
    }

    public MessageArgumentMatcher<T> onMessageType( T messageType )
    {
        this.theMessageType = messageType;
        return this;
    }

    public MessageArgumentMatcher<T> withPayload( Serializable payload )
    {
        this.payload = payload;
        return this;
    }

    /**
     * Use this for matching on headers other than HEADER_TO and HEADER_FROM, for which there are dedicated methods. The value
     * of the header is mandatory - if you don't care about it, set it to the empty string.
     */
    public MessageArgumentMatcher<T> withHeader( String headerName, String headerValue )
    {
        this.headers.add( headerName );
        this.headerValues.add( headerValue );
        return this;
    }

    @Override
    public boolean matches( Message<T> message )
    {
        if ( message == null )
        {
            return false;
        }
        boolean toMatches = to == null || to.toString().equals( message.getHeader( Message.HEADER_TO ) );
        boolean fromMatches = from == null || from.toString().equals( message.getHeader( Message.HEADER_FROM ) );
        boolean typeMatches = theMessageType == null || theMessageType == ((Message) message).getMessageType();
        boolean payloadMatches = payload == null || payload.equals( message.getPayload() );
        boolean headersMatch = true;
        for ( String header : headers )
        {
            headersMatch = headersMatch && matchHeaderAndValue( header, message.getHeader( header ) );
        }
        return fromMatches && toMatches && typeMatches && payloadMatches && headersMatch;
    }

    private boolean matchHeaderAndValue( String headerName, String headerValue )
    {
        int headerIndex = headers.indexOf( headerName );
        if ( headerIndex == -1 )
        {
            // Header not present
            return false;
        }
        if ( headerValues.get( headerIndex ).equals( "" ) )
        {
            // header name was present and value does not matter
            return true;
        }
        return headerValues.get( headerIndex ).equals( headerValue );
    }

    @Override
    public String toString()
    {
        return (theMessageType != null ? theMessageType.name() : "<no particular message type>") + "{from=" + from +
                ", to=" + to + ", payload=" + payload + "}";
    }
}
