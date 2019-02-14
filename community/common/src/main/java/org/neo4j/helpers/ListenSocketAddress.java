/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.helpers;

public class ListenSocketAddress extends SocketAddress
{
    public ListenSocketAddress( String hostname, int port )
    {
        super( hostname, port );
    }

    /**
     * Textual representation format for a listen socket address.
     * @param hostname of the address.
     * @param port of the address.
     * @return a string representing the address.
     */
    public static String listenAddress( String hostname, int port )
    {
        return new ListenSocketAddress( hostname, port ).toString();
    }
}
