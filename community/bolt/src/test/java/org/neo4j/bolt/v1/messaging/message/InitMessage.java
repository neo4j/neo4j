/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.bolt.v1.messaging.BoltRequestMessageHandler;

public class InitMessage implements RequestMessage
{
    /**
     * Factory method for obtaining INIT messages.
     */
    public static InitMessage init( String userAgent, Map<String, Object> authToken )
    {
        return new InitMessage( userAgent, authToken );
    }

    private final String userAgent;
    private final Map<String, Object> authToken;

    private InitMessage( String userAgent, Map<String, Object> authToken )
    {
        this.userAgent = userAgent;
        this.authToken = authToken;
    }

    public String userAgent()
    {
        return userAgent;
    }

    @Override
    public void dispatch( BoltRequestMessageHandler consumer )
    {
        consumer.onInit( userAgent, authToken );
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

        InitMessage that = (InitMessage) o;

        return !(userAgent != null ? !userAgent.equals( that.userAgent ) : that.userAgent != null);

    }

    @Override
    public int hashCode()
    {
        return userAgent != null ? userAgent.hashCode() : 0;
    }

    @Override
    public String toString()
    {
        return "InitMessage{" +
               "userAgent='" + userAgent + '\'' +
               '}';
    }
}
