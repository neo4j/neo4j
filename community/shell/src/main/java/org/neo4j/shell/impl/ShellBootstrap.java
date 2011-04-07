/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.server.RemoteObject;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.KernelData;
import org.neo4j.shell.StartClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class ShellBootstrap implements Serializable
{
    private final Map<String, Serializable> config;
    private final boolean enable;

    ShellBootstrap( KernelData kernel )
    {
        String shellConfig = (String) kernel.getParam( "enable_remote_shell" );
        if ( shellConfig != null )
        {
            if ( shellConfig.contains( "=" ) )
            {
                enable = true;
                config = parseShellConfigParameter( shellConfig );
            }
            else if ( Boolean.parseBoolean( shellConfig ) )
            {
                enable = true;
                config = null;
            }
            else
            {
                enable = false;
                config = null;
            }
        }
        else
        {
            enable = false;
            config = null;
        }
    }

    public ShellBootstrap( Map<String, Serializable> config )
    {
        this.config = config;
        this.enable = true;
    }

    public ShellBootstrap( String port, String name )
    {
        enable = true;
        config = new HashMap<String, Serializable>();
        config.put( StartClient.ARG_PORT, port );
        config.put( StartClient.ARG_NAME, name );
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
                throw new RuntimeException( "Invalid shell configuration '" + shellConfig
                                            + "' should be '<key1>=<value1>,<key2>=<value2>...' where key can"
                                            + " be any of [" + StartClient.ARG_PORT + ", " + StartClient.ARG_NAME
                                            + ", " + StartClient.ARG_READONLY + "]" );
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

    static String serializeStub( Remote obj )
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try
        {
            ObjectOutputStream oos = new ObjectOutputStream( os );
            oos.writeObject( RemoteObject.toStub( obj ) );
            oos.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Broken implementation!", e );
        }
        return new sun.misc.BASE64Encoder().encode( os.toByteArray() );
    }

    static ShellBootstrap deserialize( String data )
    {
        try
        {
            return (ShellBootstrap) new ObjectInputStream( new ByteArrayInputStream(
                    new sun.misc.BASE64Decoder().decodeBuffer( data ) ) ).readObject();
        }
        catch ( Exception e )
        {
            return null;
        }
    }

    @SuppressWarnings( "boxing" )
    GraphDatabaseShellServer load( GraphDatabaseService graphDb ) throws RemoteException
    {
        if ( !enable ) return null;
        return enable( new GraphDatabaseShellServer( graphDb, (Boolean) getConfig( StartClient.ARG_READONLY,
                Boolean.FALSE ) ) );
    }

    void visit( GraphDatabaseShellServer state )
    {
        // TODO: use for Registry-less connection
    }

    private Serializable getConfig( String key, Serializable defaultValue )
    {
        Serializable result = config != null ? config.get( key ) : null;
        return result != null ? result : defaultValue;
    }

    public GraphDatabaseShellServer enable( GraphDatabaseShellServer server ) throws RemoteException
    {
        Object portConfig = getConfig( StartClient.ARG_PORT, AbstractServer.DEFAULT_PORT );
        int port;
        if ( portConfig instanceof Integer )
        {
            port = (Integer) portConfig;
        }
        else if ( portConfig instanceof String )
        {
            port = Integer.parseInt( (String) portConfig );
        }
        else
        {
            throw new IllegalArgumentException( "Invalid port configuration: " + portConfig );
        }
        String name = (String) getConfig( StartClient.ARG_NAME, AbstractServer.DEFAULT_NAME );
        server.makeRemotelyAvailable( port, name );
        return server;
    }
}
