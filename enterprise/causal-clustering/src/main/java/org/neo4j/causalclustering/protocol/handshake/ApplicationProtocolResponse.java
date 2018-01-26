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

import static org.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;

public class ApplicationProtocolResponse implements ClientMessage
{
    public static final ApplicationProtocolResponse NO_PROTOCOL = new ApplicationProtocolResponse( FAILURE, "", 0 );
    private final StatusCode statusCode;
    private final String protocolName;
    private final int version;

    ApplicationProtocolResponse( StatusCode statusCode, String protocolName, int version )
    {
        this.statusCode = statusCode;
        this.protocolName = protocolName;
        this.version = version;
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
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
        ApplicationProtocolResponse that = (ApplicationProtocolResponse) o;
        return version == that.version && Objects.equals( protocolName, that.protocolName );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( protocolName, version );
    }

    public StatusCode statusCode()
    {
        return statusCode;
    }

    public String protocolName()
    {
        return protocolName;
    }

    public int version()
    {
        return version;
    }
}
