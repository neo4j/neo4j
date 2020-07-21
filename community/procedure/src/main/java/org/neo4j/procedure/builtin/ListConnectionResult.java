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
package org.neo4j.procedure.builtin;

import java.time.ZoneId;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;

public class ListConnectionResult
{
    public final String connectionId;
    public final String connectTime;
    public final String connector;
    public final String username;
    public final String userAgent;
    public final String serverAddress;
    public final String clientAddress;

    ListConnectionResult( TrackedNetworkConnection connection, ZoneId timeZone )
    {
        connectionId = connection.id();
        connectTime = ProceduresTimeFormatHelper.formatTime( connection.connectTime(), timeZone );
        connector = connection.connector();
        username = connection.username();
        userAgent = connection.userAgent();
        serverAddress = SocketAddress.format( connection.serverAddress() );
        clientAddress = SocketAddress.format( connection.clientAddress() );
    }
}
