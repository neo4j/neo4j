/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.messaging.message;

import java.util.Map;

import org.neo4j.bolt.v1.messaging.MessageHandler;

public class InitMessage implements Message
{
    private final String clientName;
    private final Map<String, Object> credentials;

    public InitMessage( String clientName, Map<String,Object> credentials )
    {
        this.clientName = clientName;
        this.credentials = credentials;
    }

    public String clientName()
    {
        return clientName;
    }

    @Override
    public <E extends Exception> void dispatch( MessageHandler<E> consumer ) throws E
    {
        consumer.handleInitMessage( clientName, credentials );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        { return true; }
        if ( o == null || getClass() != o.getClass() )
        { return false; }

        InitMessage that = (InitMessage) o;

        return !(clientName != null ? !clientName.equals( that.clientName ) : that.clientName != null);

    }

    @Override
    public int hashCode()
    {
        return clientName != null ? clientName.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "InitMessage{" +
               "clientName='" + clientName + '\'' +
               '}';
    }
}
