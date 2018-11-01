/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.BoltConnectionInfo;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientConnectionInfoTest
{
    @Test
    void connectionDetailsForBoltQuerySource()
    {
        // given
        ClientConnectionInfo clientConnection = new BoltConnectionInfo(
                "bolt-42",
                "neo4j-java-bolt-driver",
                new InetSocketAddress( "127.0.0.1", 56789 ),
                new InetSocketAddress( "127.0.0.1", 7687 ) );

        // when
        String connectionDetails = clientConnection.asConnectionDetails();

        // then
        assertEquals(
                "bolt-session\tbolt\tneo4j-java-bolt-driver\t\tclient/127.0.0.1:56789\t"
                        + "server/127.0.0.1:7687>",
                connectionDetails );
    }

    @Test
    void connectionDetailsForHttpQuerySource()
    {
        // given
        ClientConnectionInfo clientConnection =
                new HttpConnectionInfo( "http-42", "http",
                        new InetSocketAddress( "127.0.0.1", 1337 ), null, "/db/data/transaction/45/commit" );

        // when
        String connectionDetails = clientConnection.asConnectionDetails();

        // then
        assertEquals(
                "server-session\thttp\t127.0.0.1\t/db/data/transaction/45/commit",
                connectionDetails );
    }

    @Test
    void connectionDetailsForEmbeddedQuerySource()
    {
        // when
        String connectionDetails = ClientConnectionInfo.EMBEDDED_CONNECTION.asConnectionDetails();

        // then
        assertEquals( "embedded-session\t", connectionDetails );
    }
}
