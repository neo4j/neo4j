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
package org.neo4j.shell;

import java.io.Serializable;
import java.util.Map;

import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

import static org.junit.Assert.*;

public class TestClientReconnect extends AbstractShellTest
{
    @Test
    public void remoteClientAbleToReconnectAndContinue() throws Exception
    {
        makeServerRemotelyAvailable();
        ShellClient client = newRemoteClient();
        executeCommand( client, "help", "Available commands" );
        restartServer();
        makeServerRemotelyAvailable();
        executeCommand( client, "help", "Available commands" );
        client.shutdown();
    }
    
    @Test
    public void initialSessionValuesSurvivesReconnect() throws Exception
    {
        createRelationshipChain( 2 );
        makeServerRemotelyAvailable();
        Map<String, Serializable> initialSession = MapUtil.<String, Serializable>genericMap(
                "TITLE_KEYS", "test" );
        ShellClient client = newRemoteClient( initialSession );
        String name = "MyTest";
        client.evaluate( "mknode --cd" );
        client.evaluate( "set test " + name );
        assertTrue( client.getPrompt().contains( name ) );
        client.shutdown();
    }
}
