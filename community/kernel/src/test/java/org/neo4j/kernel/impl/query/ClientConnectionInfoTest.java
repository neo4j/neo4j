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
package org.neo4j.kernel.impl.query;

import java.net.InetSocketAddress;

import org.junit.Test;

import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.ShellConnectionInfo;

import static org.junit.Assert.assertEquals;

public class ClientConnectionInfoTest
{
    @Test
    public void connectionDetailsForBoltQuerySource()
    {
        // given
        ClientConnectionInfo clientConnection = new BoltConnectionInfo(
                "username",
                "neo4j-java-bolt-driver",
                new InetSocketAddress( "127.0.0.1", 56789 ),
                new InetSocketAddress( "127.0.0.1", 7687 ) )
                .withUsername( "username" );

        // when
        String connectionDetails = clientConnection.asConnectionDetails();

        // then
        assertEquals(
                "bolt-session\tbolt\tusername\tneo4j-java-bolt-driver\t\tclient/127.0.0.1:56789\t"
                        + "server/127.0.0.1:7687>\tusername",
                connectionDetails );
    }

    @Test
    public void connectionDetailsForHttpQuerySource()
    {
        // given
        ClientConnectionInfo clientConnection =
                new HttpConnectionInfo( "http", null,
                        new InetSocketAddress( "127.0.0.1", 1337 ), null, "/db/data/transaction/45/commit" )
                        .withUsername( "username" );

        // when
        String connectionDetails = clientConnection.asConnectionDetails();

        // then
        assertEquals(
                "server-session\thttp\t127.0.0.1\t/db/data/transaction/45/commit\tusername",
                connectionDetails );
    }

    @Test
    public void connectionDetailsForEmbeddedQuerySource()
    {
        // when
        String connectionDetails = ClientConnectionInfo.EMBEDDED_CONNECTION.asConnectionDetails();

        // then
        assertEquals( "embedded-session\t", connectionDetails );
    }

    @Test
    public void connectionDetailsForShellSession()
    {
        // given
        ClientConnectionInfo clientConnection = new ShellConnectionInfo( 1 ).withUsername( "FULL" );

        // when
        String connectionDetails = clientConnection.asConnectionDetails();

        // then
        assertEquals( "shell-session\tshell\t1\tFULL", connectionDetails );
    }
}
