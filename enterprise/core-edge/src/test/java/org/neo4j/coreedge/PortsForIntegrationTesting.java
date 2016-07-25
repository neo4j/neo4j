/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;

public class PortsForIntegrationTesting
{
    public static InetSocketAddress findFreeAddress() throws IOException
    {
        InetSocketAddress address = null;
        IOException ex = null;
        for ( int port = 7200; port <= 7300; port++ )
        {
            address = new InetSocketAddress( "localhost", port );

            try
            {
                new ServerSocket( address.getPort(), 100, address.getAddress() ).close();
                ex = null;
                break;
            }
            catch ( IOException e )
            {
                ex = e;
            }
        }
        if ( ex != null )
        {
            throw ex;
        }
        return address;
    }
}
