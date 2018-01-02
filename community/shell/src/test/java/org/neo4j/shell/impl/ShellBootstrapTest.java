/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.shell.impl;

import org.junit.Test;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.shell.ShellSettings;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

import static java.lang.Boolean.TRUE;
import static java.lang.String.valueOf;
import static org.mockito.Mockito.*;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class ShellBootstrapTest
{
    @Test
    public void shouldPickUpAllConfigOptions() throws Exception
    {
        // GIVEN
        String host = "test";
        int port = 1234;
        String name = "my shell";
        Config config = new Config( stringMap(
                ShellSettings.remote_shell_host.name(), host,
                ShellSettings.remote_shell_port.name(), valueOf( port ),
                ShellSettings.remote_shell_name.name(), name,
                ShellSettings.remote_shell_enabled.name(), TRUE.toString() ) );
        GraphDatabaseShellServer server = mock( GraphDatabaseShellServer.class );
        
        // WHEN
        server = new ShellBootstrap( config ).enable( server );
        
        // THEN
        verify( server ).makeRemotelyAvailable( host, port, name );
    }
}
