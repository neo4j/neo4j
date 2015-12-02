/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.docs;

import org.jsoup.nodes.Element;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.neo4j.kernel.impl.util.Codecs;

/**
 * Client: <connect>
 * Client: 60 60 B0 17
 * Client: 00 00 00 01  00 00 00 00  00 00 00 00  00 00 00 00
 * Version 1      None         None         None
 * <p/>
 * Server: 00 00 00 01
 * Choose
 * version 1
 */
public class DocExchangeExample implements Iterable<DocExchangeExample.Event>
{
    public static DocPartParser<DocExchangeExample> exchange_example =
        DocPartParser.Decoration
                .withDetailedExceptions( DocExchangeExample.class, new DocPartParser<DocExchangeExample>()
                        {
                            @Override
                            public DocExchangeExample parse( String fileName, String title, Element s )
                            {
                                return new DocExchangeExample( DocPartName.create( fileName, title ), s.text() );
                            }
                        }
                );

    public enum Type
    {
        CONNECT,
        DISCONNECT,
        SEND;
    }

    public class Event
    {

        private final String from;
        private final Type type;
        private final byte[] payload;
        private final String message;

        public Event( String from, Type type )
        {
            this( from, type, new byte[0], type.name() );
        }

        public Event( String from, Type type, byte[] payload, String message )
        {
            this.from = from;
            this.type = type;
            this.payload = payload;
            this.message = message;
        }

        public String from()
        {
            return from;
        }

        public Type type()
        {
            return type;
        }

        public byte[] payload()
        {
            return payload;
        }

        public String humanReadableMessage()
        {
            return message;
        }

        @Override
        public String toString()
        {
            return "Event{" +
                   "from='" + from + '\'' +
                   ", type=" + type +
                   ", payload=" + Arrays.toString( payload ) +
                   '}';
        }

        public boolean hasHumanReadableValue()
        {
            return message.trim().length() > 0;
        }
    }

    private final List<Event> events = new ArrayList<>();
    private final String raw;
    private final DocPartName name;

    public String name()
    {
        return name.toString();
    }

    public DocExchangeExample( DocPartName name, String raw )
    {
        this.name = name;
        this.raw = raw;

        // Generally "client" or "server", but up to the spec we're parsing
        String currentActor = null;
        String currentPayload = "";

        // If the example contains messaging, we parse out the message as well to ensure the serialization is correct
        String currentMessage = "";
        Type type = Type.SEND;

        // Parse all the lines, breaking them into events
        for ( String line : raw.split( "\n" ) )
        {
            if ( line.matches( "^[a-zA-Z]+\\s*:.*" ) )
            {
                if ( currentActor != null )
                {
                    addEvent( currentActor, currentPayload, currentMessage, type );
                }
                String[] parts = line.split( ":", 2 );
                currentActor = parts[0].trim();
                currentPayload = "";
                currentMessage = "";
                line = parts[1].trim();
                type = Type.SEND;
            }

            if ( line.matches( "^[(RUN)|(PULL_ALL)|(DISCARD_ALL)|(RECORD)|(SUCCESS)|(FAILURE)|(ACK_FAILURE)].+$" ) )
            {
                currentMessage = line;
            }
            else if ( line.matches( "^[a-fA-f0-9\\s]+$" ) )
            {
                currentPayload += line;
            }
            else if ( line.equals( "<connect>" ) )
            {
                type = Type.CONNECT;
            }
            else if ( line.equals( "<disconnect>" ) )
            {
                type = Type.DISCONNECT;
            }
            else if ( line.matches( "^\\s*[^#\\s].*$" ) )
            {
                currentMessage += line;
            }
        }

        if ( currentActor != null )
        {
            addEvent( currentActor, currentPayload, currentMessage, type );
        }
    }

    private void addEvent( String actor, String payload, String currentMessage, Type type )
    {
        events.add( new Event(
                actor, type, Codecs.decodeHexString( payload.replace( " ", "" ) ), currentMessage ) );
    }

    @Override
    public Iterator<Event> iterator()
    {
        return events.iterator();
    }

    @Override
    public String toString()
    {
        return raw;
    }
}
