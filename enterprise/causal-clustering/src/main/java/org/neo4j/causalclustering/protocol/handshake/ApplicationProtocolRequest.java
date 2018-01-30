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
package org.neo4j.causalclustering.protocol.handshake;

import java.util.Objects;
import java.util.Set;

public class ApplicationProtocolRequest implements ServerMessage
{
    private final String protocolName;
    private final Set<Integer> versions;

    ApplicationProtocolRequest( String protocolName, Set<Integer> versions )
    {
        this.protocolName = protocolName;
        this.versions = versions;
    }

    @Override
    public void dispatch( ServerMessageHandler handler )
    {
        handler.handle( this );
    }

    String protocolName()
    {
        return protocolName;
    }

    public Set<Integer> versions()
    {
        return versions;
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
        ApplicationProtocolRequest that = (ApplicationProtocolRequest) o;
        return Objects.equals( protocolName, that.protocolName ) && Objects.equals( versions, that.versions );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( protocolName, versions );
    }
}
