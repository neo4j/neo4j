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
import java.net.ServerSocket;

/**
 * Utility for picking a free port. Your OS can find a free one for you automatically.
 *
 * The OS' algorithm is approximately (on my MacBook anyways):
 *
 *  - remember ports that are currently in use
 *  - maintain a pointer to the last used/ next free ephemeral/ dynamic port (49152 to 65535 on my MacBook)
 *  - hand out next free port on request and increment the pointer
 *    - wrap around when you get to the end (65535)
 *
 *  Of course there is a race here where, between requesting a port and actually putting it to use, enough pressure on
 *  OS' port picking mechanism would mean it had gone to the end, wrapped around, caught up, handed the still free port
 *  to someone else, and they had put it to use before you did.
 *
 *  But back in the real world this is just a lovely singleton to use.
 *
 *  See also https://en.wikipedia.org/wiki/Ephemeral_port
 */
public class PortPicker
{
    /**
     * Picks a free port.
     *
     * @return a free port.
     */
    public static int pickPort()
    {
        try
        {
            ServerSocket ss = new ServerSocket(0 ); // OS gonna OS.
            int port = ss.getLocalPort();
            ss.close();
            return port;
        }
        catch ( IOException e )
        {
            throw new IllegalStateException( "This won't ever happen - LWN" );
        }
    }
}
