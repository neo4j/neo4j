/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

public class WebTestUtils
{

    private static boolean available( int port )
    {
        if ( port < 1111 || port > 9999 )
        {
            throw new IllegalArgumentException( "Invalid start port: " + port );
        }

        ServerSocket ss = null;
        DatagramSocket ds = null;
        try
        {
            ss = new ServerSocket( port );
            ss.setReuseAddress( true );
            ds = new DatagramSocket( port );
            ds.setReuseAddress( true );
            return true;
        }
        catch ( IOException e )
        {
        }
        finally
        {
            if ( ds != null )
            {
                ds.close();
            }

            if ( ss != null )
            {
                try
                {
                    ss.close();
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }

        return false;
    }

    public static int nextAvailablePortNumber()
    {
        int nonPriveledgedPortNumber = 1111;
        while ( !available( nonPriveledgedPortNumber ) )
        {
            nonPriveledgedPortNumber++;
        }
        return nonPriveledgedPortNumber;
    }
}
