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
package org.neo4j.cluster.protocol;

import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

import java.io.Serializable;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;

public class MessageArgumentMatcher<T extends MessageType> extends ArgumentMatcher<Message<T>>
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
     * Use this for matching on headers other than TO and FROM, for which there are dedicated methods. The value
     * of the header is mandatory - if you don't care about it, set it to the empty string.
     */
    public MessageArgumentMatcher<T> withHeader( String headerName, String headerValue )
    {
        this.headers.add( headerName );
        this.headerValues.add( headerValue );
        return this;
    }

    @Override
    public boolean matches( Object message )
    {
        if ( message == null || !( message instanceof Message ) )
        {
            return false;
        }
        if ( message == this )
        {
            return true;
        }
        Message toMatchAgainst = (Message) message;
        boolean toMatches = to == null ? true : to.toString().equals( toMatchAgainst.getHeader( Message.TO ) );
        boolean fromMatches = from == null ? true : from.toString().equals( toMatchAgainst.getHeader( Message.FROM ) );
        boolean typeMatches = theMessageType == null ? true : theMessageType == toMatchAgainst.getMessageType();
        boolean payloadMatches = payload == null ? true : payload.equals( toMatchAgainst.getPayload() );
        boolean headersMatch = true;
        for ( String header : headers )
        {
            headersMatch = headersMatch && matchHeaderAndValue( header, toMatchAgainst.getHeader( header ) );
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
    public void describeTo( Description description )
    {
        description.appendText(
                (theMessageType != null ? theMessageType.name() : "<no particular message type>") +
                "{from=" + from + ", to=" + to + ", payload=" + payload + "}" );
    }
}
