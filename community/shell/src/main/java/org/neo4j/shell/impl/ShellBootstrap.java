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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.shell.ShellSettings;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class ShellBootstrap implements Serializable
{
    private final boolean enable;
    private String host;
    private final int port;
    private final String name;
    private final boolean read_only;

    ShellBootstrap( Config config )
    {
        this.enable = config.get( ShellSettings.remote_shell_enabled );
        this.host = config.get( ShellSettings.remote_shell_host );
        this.port = config.get( ShellSettings.remote_shell_port );
        this.name = config.get( ShellSettings.remote_shell_name );
        this.read_only = config.get( ShellSettings.remote_shell_read_only );
    }

    public ShellBootstrap( int port, String name )
    {
        enable = true;
        this.port = port;
        this.name = name;
        this.read_only = false;
    }

    public String serialize()
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream( os );
            oos.writeObject( this );
            oos.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Broken implementation!", e );
        }
        return new sun.misc.BASE64Encoder().encode( os.toByteArray() );
    }

    @SuppressWarnings("boxing")
    GraphDatabaseShellServer load( GraphDatabaseAPI graphDb ) throws RemoteException
    {
        if ( !enable )
        {
            return null;
        }
        return enable( new GraphDatabaseShellServer( graphDb, read_only ) );
    }

    public GraphDatabaseShellServer enable( GraphDatabaseShellServer server ) throws RemoteException
    {
        server.makeRemotelyAvailable( host, port, name );
        return server;
    }
}
