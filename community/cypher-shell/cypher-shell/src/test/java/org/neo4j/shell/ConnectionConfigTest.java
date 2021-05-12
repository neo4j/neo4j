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
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import org.neo4j.shell.cli.Encryption;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

class ConnectionConfigTest
{
    private final ConnectionConfig config = new ConnectionConfig( "bolt", "localhost", 1, "bob",
            "pass", Encryption.DEFAULT, "db" );

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
    @SetEnvironmentVariable( key = ConnectionConfig.USERNAME_ENV_VAR, value = "alice" )
    void usernameDefaultsToEnvironmentVar()
    {
        ConnectionConfig configWithEmptyParams = new ConnectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME );
        assertEquals( "alice", configWithEmptyParams.username() );
    }

    @Test
    void password()
    {
        assertEquals( "pass", config.password() );
    }

    @Test
    @SetEnvironmentVariable( key = ConnectionConfig.PASSWORD_ENV_VAR, value = "ssap" )
    void passwordDefaultsToEnvironmentVar()
    {
        ConnectionConfig configWithEmptyParams = new ConnectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME );
        assertEquals( "ssap", configWithEmptyParams.password() );
    }

    @Test
    void database()
    {
        assertEquals( "db", config.database() );
    }

    @Test
    @SetEnvironmentVariable( key = ConnectionConfig.DATABASE_ENV_VAR, value = "funnyDB" )
    void databaseDefaultsToEnvironmentVar()
    {
        ConnectionConfig configWithEmptyParams = new ConnectionConfig( "bolt", "localhost", 1, "",
                                                                       "", Encryption.DEFAULT, ABSENT_DB_NAME );
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
        assertEquals( Encryption.DEFAULT, new ConnectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME ).encryption() );
        assertEquals( Encryption.TRUE, new ConnectionConfig( "bolt", "", -1, "", "", Encryption.TRUE, ABSENT_DB_NAME ).encryption() );
        assertEquals( Encryption.FALSE, new ConnectionConfig( "bolt", "", -1, "", "", Encryption.FALSE, ABSENT_DB_NAME ).encryption() );
    }
}
