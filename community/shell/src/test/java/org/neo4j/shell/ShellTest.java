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
package org.neo4j.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_REMOTE_SHELL;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.impl.ShellBootstrap;
import org.neo4j.shell.impl.ShellServerExtension;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;

public class ShellTest
{
    private AppCommandParser parse( final String line ) throws Exception
    {
        return new AppCommandParser( new GraphDatabaseShellServer( null ),
                line );
    }

    @Test
    public void testParserEasy() throws Exception
    {
        AppCommandParser parser = this.parse( "ls -la" );
        assertEquals( "ls", parser.getAppName() );
        assertEquals( 2, parser.options().size() );
        assertTrue( parser.options().containsKey( "l" ) );
        assertTrue( parser.options().containsKey( "a" ) );
        assertTrue( parser.arguments().isEmpty() );
    }

    @Test
    public void testParserArguments() throws Exception
    {
        AppCommandParser parser = this
        .parse( "set -t java.lang.Integer key value" );
        assertEquals( "set", parser.getAppName() );
        assertTrue( parser.options().containsKey( "t" ) );
        assertEquals( "java.lang.Integer", parser.options().get( "t" ) );
        assertEquals( 2, parser.arguments().size() );
        assertEquals( "key", parser.arguments().get( 0 ) );
        assertEquals( "value", parser.arguments().get( 1 ) );

        assertException( "set -tsd" );
    }

    @Test
    public void testEnableRemoteShell() throws Exception
    {
        int port = 8085;
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(
                "target/shell-neo", stringMap( ENABLE_REMOTE_SHELL, "port=" + port ) );
        ShellLobby.newClient( port );
        graphDb.shutdown();
    }

    @Test
    public void testEnableServerOnDefaultPort() throws Exception
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "target/shell-neo", MapUtil.stringMap( Config.ENABLE_REMOTE_SHELL, "true" ) );
        try
        {
            ShellLobby.newClient();
        }
        finally
        {
            graphDb.shutdown();
        }
    }

    @Test
    public void canConnectAsAgent() throws Exception
    {
        Integer port = Integer.valueOf( 1234 );
        String name = "test-shell";
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( "target/shell-neo" );
        try
        {
            new ShellServerExtension().loadAgent( new ShellBootstrap( port.toString(), name ).serialize() );
        }
        finally
        {
            graphDb.shutdown();
        }
        ShellLobby.newClient( port.intValue(), name );
    }

    @Test
    public void testRemoveReferenceNode() throws Exception
    {
        final GraphDatabaseShellServer server = new GraphDatabaseShellServer( "target/shell-neo", false, null );
        ShellClient client = new SameJvmClient( server );

        Documenter doc = new Documenter("sample session", client);
        doc.add("pwd", "", "where are we?");
        doc.add("set name \"Jon\"", "", "On the current node, set the key \"name\" to value \"Jon\"");
        doc.add("start n=(0) return n", "Jon", "send a cypher query");
        doc.add("mkrel -c -d i -t LIKES --np \"{'app':'foobar'}\"", "", "make an incoming relationship of type LIKES, create the end node with the node properties specified.");
        doc.add("ls", "1", "where are we?");
        doc.add("cd 1", "", "change to the newly created node");
        doc.add("ls -avr", "LIKES", "list relationships, including relationshship id");

        doc.add( "mkrel -c -d i -t KNOWS --np \"{'name':'Bob'}\"", "", "create one more KNOWS relationship and the end node" );
        doc.add( "pwd", "0", "print current history stack" );
        doc.add( "ls -avr", "KNOWS", "verbose list relationships" );
        doc.run();
        //TODO: implement support for removing root node and previous nodes in the history stack of PWD
        //client.getServer().interpretLine( "rmnode -f 0", client.session(), client.getOutput() );
        //        client.getServer().interpretLine( "cd", client.session(), client.getOutput() );
        //        client.getServer().interpretLine( "pwd", client.session(), client.getOutput() );
        server.shutdown();
    }

    @Test
    public void testMatrix() throws Exception
    {
        final GraphDatabaseShellServer server = new GraphDatabaseShellServer( "target/shell-matrix", false, "src/test/resources/autoindex.properties" );
        ShellClient client = new SameJvmClient( server );

        Documenter doc = new Documenter("a matrix example", client);
        doc.add( "mkrel -t ROOT -c -v", "created",
        "create the Thomas Andersson node" );
        doc.add("cd 1", "", "go to the new node");
        doc.add("set name \"Thomas Andersson\"", "", "set the name property");
        doc.add( "mkrel -t KNOWS -cv", "", "create Thomas direct friends" );
        doc.add("cd 2", "", "go to the new node");
        doc.add("set name \"Trinity\"", "", "set the name property");
        doc.add("cd ..", "", "go back in the history stack");
        doc.add("mkrel -t KNOWS -cv", "", "create Thomas direct friends");
        doc.add("cd 3", "", "go to the new node");
        doc.add("set name \"Morpheus\"", "", "set the name property");
        doc.add("mkrel -t KNOWS -n 2", "", "create relationship to Trinity");

        doc.add("ls -rv", "", "list the relationships of node 3");
        doc.add("cd -r 2", "", "change the current position to relationship #2");

        doc.add( "set -t int age 3", "", "set the age property on the relationship" );
        doc.add( "cd ..", "", "back to Morpheus" );
        doc.add( "cd -r 3", "", "next relationsip" );
        doc.add( "set -t int age 90", "", "set the age property on the relationship" );
        doc.add( "cd start", "", "position to the start node of the current relationship" );

        doc.add( "","","We're now standing on Morpheus node, so let's create the rest of the friends." );
        doc.add( "mkrel -t KNOWS -c", "", "new node" );
        doc.add( "ls -r", "", "list relationships on the current node" );
        doc.add( "cd 4", "", "go to Cypher" );
        doc.add( "set name Cypher", "", "set the name" );
        doc.add( "mkrel -ct KNOWS", "", "create new node from Cypher" );
        //TODO: how to list outgoing relationships?
        //doc.add( "ls -rd out", "", "list relationships" );
        doc.add( "ls -r", "", "list relationships" );
        doc.add( "cd 5", "", "go to the Agent Smith node" );
        doc.add( "set name \"Agent Smith\"", "", "set the name" );
        doc.add( "mkrel -cvt CODED_BY", "", "outgoing relationship and new node" );
        doc.add( "cd 6", "", "go there" );
        doc.add( "set name \"The Architect\"", "", "set the name" );
        doc.add( "cd", "", "go to the first node in the history stack" );

        doc.add( "","","Now, let's ask some questions" );
        doc.add( "start morpheus = (node_auto_index, name, 'Morpheus') " +
                "match morpheus-[:KNOWS]-zionist " +
                "return zionist.name",
                "",
                "Morpheus' friends, looking up Morpheus by name in the Neo4j autoindex" );
        doc.run();
        server.shutdown();
    }

    private void assertException( final String command )
    {
        try
        {
            this.parse( command );
            fail( "Should fail" );
        }
        catch ( Exception e )
        {
            // Good
        }
    }
}