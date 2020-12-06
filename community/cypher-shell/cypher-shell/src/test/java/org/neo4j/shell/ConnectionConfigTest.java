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
package org.neo4j.shell;

import org.junit.Test;

import org.neo4j.driver.v1.Config;
import org.neo4j.shell.log.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ConnectionConfigTest
{
    private Logger logger = mock( Logger.class );
    private ConnectionConfig config = new ConnectionConfig( "bolt://", "localhost", 1, "bob",
                                                            "pass", false );

    @Test
    public void scheme() throws Exception
    {
        assertEquals( "bolt://", config.scheme() );
    }

    @Test
    public void host() throws Exception
    {
        assertEquals( "localhost", config.host() );
    }

    @Test
    public void port() throws Exception
    {
        assertEquals( 1, config.port() );
    }

    @Test
    public void username() throws Exception
    {
        assertEquals( "bob", config.username() );
    }

    @Test
    public void password() throws Exception
    {
        assertEquals( "pass", config.password() );
    }

    @Test
    public void driverUrlDefaultScheme() throws Exception
    {
        assertEquals( "bolt://localhost:1", config.driverUrl() );
    }

    @Test
    public void encryption()
    {
        assertEquals( Config.EncryptionLevel.REQUIRED,
                      new ConnectionConfig( "bolt://", "", -1, "", "", true ).encryption() );
        assertEquals( Config.EncryptionLevel.NONE,
                      new ConnectionConfig( "bolt://", "", -1, "", "", false ).encryption() );
    }
}
