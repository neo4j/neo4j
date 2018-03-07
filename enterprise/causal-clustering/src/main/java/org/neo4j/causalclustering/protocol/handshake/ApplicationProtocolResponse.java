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

import static org.neo4j.causalclustering.protocol.handshake.StatusCode.FAILURE;

public class ApplicationProtocolResponse extends BaseProtocolResponse implements ClientMessage
{
    public static final ApplicationProtocolResponse NO_PROTOCOL = new ApplicationProtocolResponse( FAILURE, "", 0 );

    ApplicationProtocolResponse( StatusCode statusCode, String protocolName, int version )
    {
        super( statusCode, protocolName, version );
    }

    @Override
    public void dispatch( ClientMessageHandler handler )
    {
        handler.handle( this );
    }
}
