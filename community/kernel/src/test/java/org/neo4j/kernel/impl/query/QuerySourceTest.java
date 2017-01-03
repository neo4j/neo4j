/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import java.net.InetSocketAddress;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class QuerySourceTest
{
    @Test
    public void connectionDetailsForBoltQuerySource() throws Exception
    {
        // given
        QuerySource querySource = new QuerySource.BoltSession(
                "username",
                "neo4j-java-bolt-driver",
                new InetSocketAddress( "127.0.0.1", 56789 ),
                new InetSocketAddress( "127.0.0.1", 7687 ) )
                .withUsername( "username" );

        // when
        String connectionDetails = querySource.asConnectionDetails();

        // then
        assertEquals(
                "bolt-session\tbolt\tusername\tneo4j-java-bolt-driver\t\tclient/127.0.0.1:56789\t"
                        + "server/127.0.0.1:7687>\tusername",
                connectionDetails );
    }

    @Test
    public void connectionDetailsForHttpQuerySource() throws Exception
    {
        // given
        QuerySource querySource =
                new QuerySource.ServerSession( "http", "127.0.0.1", "/db/data/transaction/45/commit" )
                        .withUsername( "username" );

        // when
        String connectionDetails = querySource.asConnectionDetails();

        // then
        assertEquals(
                "server-session\thttp\t127.0.0.1\t/db/data/transaction/45/commit\tusername",
                connectionDetails );
    }

    @Test
    public void connectionDetailsForEmbeddedQuerySource() throws Exception
    {
        // when
        String connectionDetails = QuerySource.EMBEDDED_SESSION.asConnectionDetails();

        // then
        assertEquals( "embedded-session\t", connectionDetails );
    }

    @Test
    public void connectionDetailsForShellSession() throws Exception
    {
        // given
        QuerySource querySource = new QuerySource.ShellSession( 1 ).withUsername( "FULL" );

        // when
        String connectionDetails = querySource.asConnectionDetails();

        // then
        assertEquals( "shell-session\tshell\t1\tFULL", connectionDetails );
    }
}
