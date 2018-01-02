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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.impl.SimpleAppServer;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.ImpermanentGraphDatabase;

import static java.util.regex.Pattern.compile;

import static org.junit.Assert.assertTrue;
import static org.neo4j.shell.Variables.PROMPT_KEY;

public class ServerClientInteractionTest
{

    private GraphDatabaseAPI db;

    @Test
    public void shouldConsiderAndInterpretCustomClientPrompt() throws Exception
    {
        // GIVEN
        client.setSessionVariable( PROMPT_KEY, "MyPrompt \\d \\t$ " );
        
        // WHEN
        Response response = server.interpretLine( client.getId(), "", out );
        
        // THEN
        String regexPattern = "MyPrompt .{1,3} .{1,3} \\d{1,2} \\d{2}:\\d{2}:\\d{2}\\$";
        assertTrue( "Prompt from server '" + response.getPrompt() + "' didn't match pattern '" + regexPattern + "'",
                compile( regexPattern ).matcher( response.getPrompt() ).find() );
    }
    
    private SimpleAppServer server;
    private ShellClient client;
    private SilentLocalOutput out;
    
    @Before
    public void before() throws Exception
    {
        db = new ImpermanentGraphDatabase(  );
        server = new GraphDatabaseShellServer( db );
        out = new SilentLocalOutput();
        client = new SameJvmClient( MapUtil.<String,Serializable>genericMap(), server, out, InterruptSignalHandler.getHandler() );
    }

    @After
    public void after() throws Exception
    {
        server.shutdown();
        db.shutdown();
    }
}
