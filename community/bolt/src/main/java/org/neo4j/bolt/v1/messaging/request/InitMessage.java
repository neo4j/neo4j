/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v1.messaging.request;

import java.util.Map;
import java.util.Objects;

import org.neo4j.bolt.messaging.RequestMessage;

import static java.util.Objects.requireNonNull;

public class InitMessage implements RequestMessage
{
    public static final byte SIGNATURE = 0x01;

    private final String userAgent;
    private final Map<String,Object> authToken;

    public InitMessage( String userAgent, Map<String,Object> authToken )
    {
        this.userAgent = requireNonNull( userAgent );
        this.authToken = requireNonNull( authToken );
    }

    public String userAgent()
    {
        return userAgent;
    }

    public Map<String,Object> authToken()
    {
        return authToken;
    }

    @Override
    public boolean safeToProcessInAnyState()
    {
        return false;
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
        return Objects.equals( userAgent, that.userAgent ) &&
               Objects.equals( authToken, that.authToken );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( userAgent, authToken );
    }

    @Override
    public String toString()
    {
        return "INIT " + userAgent + ' ' + authToken;
    }
}
