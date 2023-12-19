/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.com;

import java.net.InetSocketAddress;

public class ServerUtil
{

    private ServerUtil()
    {
    }

    /**
     * Figure out the host string of a given socket address, similar to the Java 7 InetSocketAddress.getHostString().
     *
     * Calls to this should be replace once Neo4j is Java 7 only.
     *
     * @param socketAddress
     * @return
     */
    public static String getHostString( InetSocketAddress socketAddress )
    {
        if ( socketAddress.isUnresolved() )
        {
            return socketAddress.getHostName();
        }
        else
        {
            return socketAddress.getAddress().getHostAddress();
        }
    }
}
