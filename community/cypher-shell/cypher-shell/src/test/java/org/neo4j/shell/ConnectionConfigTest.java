/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.shell.cli.Encryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.shell.ConnectionConfig.connectionConfig;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

class ConnectionConfigTest
{
    private final Map<String,String> env = new HashMap<>();
    private final Environment testEnvironment = new Environment( env );
    private final ConnectionConfig config = connectionConfig( "bolt", "localhost", 1, "bob", "pass", Encryption.DEFAULT, "db", new Environment() );

    @Test
    void scheme()
    {
        assertEquals( "bolt", config.scheme() );
    }

    @Test
    void host()
    {
        assertEquals( "localhost", config.host() );
    }

    @Test
    void port()
    {
        assertEquals( 1, config.port() );
    }

    @Test
    void username()
    {
        assertEquals( "bob", config.username() );
    }

    @Test
    void usernameDefaultsToEnvironmentVar()
    {
        env.put( ConnectionConfig.USERNAME_ENV_VAR, "alice" );
        ConnectionConfig configWithEmptyParams = connectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME, testEnvironment );
        assertEquals( "alice", configWithEmptyParams.username() );
    }

    @Test
    void password()
    {
        assertEquals( "pass", config.password() );
    }

    @Test
    void passwordDefaultsToEnvironmentVar()
    {
        env.put( ConnectionConfig.PASSWORD_ENV_VAR, "ssap" );
        ConnectionConfig configWithEmptyParams = connectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME, testEnvironment );
        assertEquals( "ssap", configWithEmptyParams.password() );
    }

    @Test
    void database()
    {
        assertEquals( "db", config.database() );
    }

    @Test
    void databaseDefaultsToEnvironmentVar()
    {
        env.put( ConnectionConfig.DATABASE_ENV_VAR, "funnyDB" );
        ConnectionConfig configWithEmptyParams = connectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME, testEnvironment );
        assertEquals( "funnyDB", configWithEmptyParams.database() );
    }

    @Test
    void driverUrlDefaultScheme()
    {
        assertEquals( "bolt://localhost:1", config.driverUrl() );
    }

    @Test
    void encryption()
    {
        assertEquals( Encryption.DEFAULT, connectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME, new Environment() ).encryption() );
        assertEquals( Encryption.TRUE, connectionConfig( "bolt", "", -1, "", "", Encryption.TRUE, ABSENT_DB_NAME, new Environment() ).encryption() );
        assertEquals( Encryption.FALSE, connectionConfig( "bolt", "", -1, "", "", Encryption.FALSE, ABSENT_DB_NAME, new Environment() ).encryption() );
    }
}
