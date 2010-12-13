/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.shell.impl;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.KernelExtension;
import org.neo4j.shell.StartClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

@Service.Implementation( KernelExtension.class )
public final class ShellServerExtension extends KernelExtension
{
    public ShellServerExtension()
    {
        super( "shell" );
    }

    @Override
    protected void load( KernelData kernel )
    {
        String shellConfig = (String) kernel.getParam( "enable_remote_shell" );
        if ( shellConfig != null )
        {
            if ( shellConfig.contains( "=" ) )
            {
                enableRemoteShell( kernel, parseShellConfigParameter( shellConfig ) );
            }
            else if ( Boolean.parseBoolean( shellConfig ) )
            {
                enableRemoteShell( kernel, null );
            }
        }
    }

    @Override
    protected void unload( KernelData kernel )
    {
        GraphDatabaseShellServer server = getServer( kernel );
        if ( server != null )
        {
            server.shutdown();
        }
    }

    @SuppressWarnings( "boxing" )
    public void enableRemoteShell( KernelData kernel, Map<String, Serializable> config )
    {
        GraphDatabaseShellServer server = getServer( kernel );
        if ( server != null )
        {
            throw new IllegalStateException( "Shell already enabled" );
        }
        if ( config == null ) config = Collections.<String, Serializable>emptyMap();
        try
        {
            server = new GraphDatabaseShellServer( kernel.graphDatabase(), (Boolean) getConfig(
                    config, StartClient.ARG_READONLY, Boolean.FALSE ) );
            int port = (Integer) getConfig( config, StartClient.ARG_PORT,
                    AbstractServer.DEFAULT_PORT );
            String name = (String) getConfig( config, StartClient.ARG_NAME,
                    AbstractServer.DEFAULT_NAME );
            server.makeRemotelyAvailable( port, name );
        }
        catch ( Exception ex )
        {
            if ( server != null ) server.shutdown();
            throw new IllegalStateException( "Can't start remote Neo4j shell", ex );
        }
        setServer( kernel, server );
    }

    private void setServer( KernelData kernel, final GraphDatabaseShellServer server )
    {
        kernel.setState( this, server );
    }

    private GraphDatabaseShellServer getServer( KernelData kernel )
    {
        return (GraphDatabaseShellServer) kernel.getState( this );
    }

    @SuppressWarnings( "boxing" )
    private static Map<String, Serializable> parseShellConfigParameter( String shellConfig )
    {
        Map<String, Serializable> map = new HashMap<String, Serializable>();
        for ( String keyValue : shellConfig.split( "," ) )
        {
            String[] splitted = keyValue.split( "=" );
            if ( splitted.length != 2 )
            {
                throw new RuntimeException(
                        "Invalid shell configuration '" + shellConfig
                                + "' should be '<key1>=<value1>,<key2>=<value2>...' where key can"
                                + " be any of [" + StartClient.ARG_PORT + ", " +
                                StartClient.ARG_NAME + ", " + StartClient.ARG_READONLY + "]" );
            }
            String key = splitted[0].trim();
            Serializable value = splitted[1];
            if ( key.equals( StartClient.ARG_PORT ) )
            {
                value = Integer.parseInt( splitted[1] );
            }
            else if ( key.equals( StartClient.ARG_READONLY ) )
            {
                value = Boolean.parseBoolean( splitted[1] );
            }
            map.put( key, value );
        }
        return map;
    }

    private Serializable getConfig( Map<String, Serializable> config, String key,
            Serializable defaultValue )
    {
        Serializable result = config.get( key );
        return result != null ? result : defaultValue;
    }
}
