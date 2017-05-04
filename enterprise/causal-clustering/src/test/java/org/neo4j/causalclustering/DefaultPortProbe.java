/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class DefaultPortProbe implements PortProbe
{
    @Override
    public boolean isOccupied( int port )
    {
        // test binding on wildcard
        try ( ServerSocket serverSocket = new ServerSocket() )
        {
            serverSocket.bind( new InetSocketAddress( port ) );
        }
        catch ( IOException e )
        {
            return true;
        }

        // test binding on loopback
        try ( ServerSocket serverSocket = new ServerSocket() )
        {
            serverSocket.bind( new InetSocketAddress( InetAddress.getLoopbackAddress(), port ) );
        }
        catch ( IOException e )
        {
            return true;
        }

        return false;
    }
}
