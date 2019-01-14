/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.shell.impl.SimpleAppServer;

import static org.junit.Assert.assertTrue;

public class ClientReconnectIT extends AbstractShellIT
{
    @Before
    public void setUp()
    {
        makeServerRemotelyAvailable();
    }

    @Test
    public void remoteClientAbleToReconnectAndContinue() throws Exception
    {
        ShellClient client = newRemoteClient();
        executeCommand( client, "help", "Available commands" );
        int serverPort = this.remotelyAvailableOnPort;
        restartServer();
        makeRemoveAvailableOnPort( serverPort );
        executeCommand( client, "help", "Available commands" );
        client.shutdown();
    }

    @Test
    public void initialSessionValuesSurvivesReconnect() throws Exception
    {
        createRelationshipChain( 2 );
        Map<String,Serializable> initialSession = MapUtil.genericMap( "TITLE_KEYS", "test" );
        ShellClient client = newRemoteClient( initialSession );
        String name = "MyTest";
        client.evaluate( "mknode --cd" );
        client.evaluate( "set test " + name );
        assertTrue( client.getPrompt().contains( name ) );
        client.shutdown();
    }

    private void makeRemoveAvailableOnPort( int serverPort )
    {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 30 );
        do
        {
            try
            {
                shellServer.makeRemotelyAvailable( serverPort, SimpleAppServer.DEFAULT_NAME );
                return;
            }
            catch ( Throwable t )
            {
                //ignore
            }
        }
        while ( System.currentTimeMillis() < deadline );
        throw new RuntimeException( "Not able to start shell server on desired port for more then 30 seconds." );
    }
}
