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

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.log.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

public class ConnectionConfigTest
{

    @Rule
    public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

    private Logger logger = mock( Logger.class );
    private ConnectionConfig config = new ConnectionConfig( "bolt", "localhost", 1, "bob",
                                                            "pass", Encryption.DEFAULT, "db" );

    @Test
    public void scheme()
    {
        assertEquals( "bolt", config.scheme() );
    }

    @Test
    public void host()
    {
        assertEquals( "localhost", config.host() );
    }

    @Test
    public void port()
    {
        assertEquals( 1, config.port() );
    }

    @Test
    public void username()
    {
        assertEquals( "bob", config.username() );
    }

    @Test
    public void usernameDefaultsToEnvironmentVar()
    {
        environmentVariables.set( ConnectionConfig.USERNAME_ENV_VAR, "alice" );
        ConnectionConfig configWithEmptyParams = new ConnectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME );
        assertEquals( "alice", configWithEmptyParams.username() );
    }

    @Test
    public void password()
    {
        assertEquals( "pass", config.password() );
    }

    @Test
    public void passwordDefaultsToEnvironmentVar()
    {
        environmentVariables.set( ConnectionConfig.PASSWORD_ENV_VAR, "ssap" );
        ConnectionConfig configWithEmptyParams = new ConnectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME );
        assertEquals( "ssap", configWithEmptyParams.password() );
    }

    @Test
    public void database()
    {
        assertEquals( "db", config.database() );
    }

    @Test
    public void databaseDefaultsToEnvironmentVar()
    {
        environmentVariables.set( ConnectionConfig.DATABASE_ENV_VAR, "funnyDB" );
        ConnectionConfig configWithEmptyParams = new ConnectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME );
        assertEquals( "funnyDB", configWithEmptyParams.database() );
    }

    @Test
    public void driverUrlDefaultScheme()
    {
        assertEquals( "bolt://localhost:1", config.driverUrl() );
    }

    @Test
    public void encryption()
    {
        assertEquals( Encryption.DEFAULT, new ConnectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME ).encryption() );
        assertEquals( Encryption.TRUE, new ConnectionConfig( "bolt", "", -1, "", "", Encryption.TRUE, ABSENT_DB_NAME ).encryption() );
        assertEquals( Encryption.FALSE, new ConnectionConfig( "bolt", "", -1, "", "", Encryption.FALSE, ABSENT_DB_NAME ).encryption() );
    }
}
