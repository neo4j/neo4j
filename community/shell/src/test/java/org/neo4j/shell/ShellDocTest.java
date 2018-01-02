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

import org.junit.Test;

import java.io.PrintWriter;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.RemoteClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.System.lineSeparator;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.shell.ShellLobby.NO_INITIAL_SESSION;
import static org.neo4j.shell.ShellLobby.remoteLocation;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphViz;

public class ShellDocTest
{
    private AppCommandParser parse( final String line ) throws Exception
    {
        return new AppCommandParser( new GraphDatabaseShellServer( null ), line );
    }

    @Test
    public void testParserEasy() throws Exception
    {
        AppCommandParser parser = parse( "ls -la" );
        assertEquals( "ls", parser.getAppName() );
        assertEquals( 2, parser.options().size() );
        assertTrue( parser.options().containsKey( "l" ) );
        assertTrue( parser.options().containsKey( "a" ) );
        assertTrue( parser.arguments().isEmpty() );
    }

    @Test
    public void parsingUnrecognizedOptionShouldFail() throws Exception
    {
        String unrecognizedOption = "unrecognized-option";
        try
        {
            parse( "ls --" + unrecognizedOption );
            fail( "Should fail when encountering unrecognized option" );
        }
        catch ( ShellException e )
        {
            assertThat( e.getMessage(), containsString( unrecognizedOption ) );
        }
    }

    @Test
    public void testParserArguments() throws Exception
    {
        AppCommandParser parser = parse( "set -t java.lang.Integer key value" );
        assertEquals( "set", parser.getAppName() );
        assertTrue( parser.options().containsKey( "t" ) );
        assertEquals( "java.lang.Integer", parser.options().get( "t" ) );
        assertEquals( 2, parser.arguments().size() );
        assertEquals( "key", parser.arguments().get( 0 ) );
        assertEquals( "value", parser.arguments().get( 1 ) );
        assertShellException( "set -tsd" );
    }

    @Test
    public void testEnableRemoteShellOnCustomPort() throws Exception
    {
        int port = 8085;
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().
                newImpermanentDatabaseBuilder().
                setConfig( ShellSettings.remote_shell_enabled, "true" ).
                setConfig( ShellSettings.remote_shell_port, "" + port ).
                newGraphDatabase();
        RemoteClient client = new RemoteClient( NO_INITIAL_SESSION, remoteLocation( port ), new CollectingOutput() );
        client.evaluate( "help" );
        client.shutdown();
        graphDb.shutdown();
    }

    @Test
    public void testEnableServerOnDefaultPort() throws Exception
    {
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().
                setConfig( ShellSettings.remote_shell_enabled, Settings.TRUE ).
                newGraphDatabase();
        try
        {
            RemoteClient client = new RemoteClient( NO_INITIAL_SESSION, remoteLocation(), new CollectingOutput() );
            client.evaluate( "help" );
            client.shutdown();
        }
        finally
        {
            graphDb.shutdown();
        }
    }

    @Test
    public void testRemoveReferenceNode() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        final GraphDatabaseShellServer server = new GraphDatabaseShellServer( db, false );

        Documenter doc = new Documenter( "sample session", server );
        doc.add( "mknode --cd", "", "Create a node");
        doc.add( "pwd", "", "where are we?" );
        doc.add( "set name \"Jon\"", "", "On the current node, set the key \"name\" to value \"Jon\"" );
        doc.add( "match n where id(n) = 0 return n;", "Jon", "send a cypher query" );
        doc.add( "mkrel -c -d i -t LIKES --np \"{'app':'foobar'}\"", "", "make an incoming relationship of type " +
                "LIKES, create the end node with the node properties specified." );
        doc.add( "ls", "1", "where are we?" );
        doc.add( "cd 1", "", "change to the newly created node" );
        doc.add( "ls -avr", "LIKES", "list relationships, including relationship id" );

        doc.add( "mkrel -c -d i -t KNOWS --np \"{'name':'Bob'}\"", "", "create one more KNOWS relationship and the " +
                "end node" );
        doc.add( "pwd", "0", "print current history stack" );
        doc.add( "ls -avr", "KNOWS", "verbose list relationships" );
        db.beginTx();
        doc.run();
        doc.add( "rmnode -f 0", "", "delete node 0" );
        doc.add( "cd 0", "", "cd back to node 0" );
        doc.add( "pwd", "(?)", "the node doesn't exist now" );
        doc.add( "mknode --cd --np \"{'name':'Neo'}\"", "", "create a new node and go to it" );
        server.shutdown();
        db.shutdown();
    }

    @Test
    public void testDumpCypherResultSimple() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        final GraphDatabaseShellServer server = new GraphDatabaseShellServer( db, false );

        try ( Transaction tx = db.beginTx() )
        {
            Documenter doc = new Documenter( "simple cypher result dump", server );
            doc.add( "mknode --cd --np \"{'name':'Neo'}\"", "", "create a new node and go to it" );
            doc.add( "mkrel -c -d i -t LIKES --np \"{'app':'foobar'}\"", "", "create a relationship" );
            doc.add( "dump MATCH (n)-[r]-(m) WHERE n = {self} return n,r,m;",
                    "create (_0 {`name`:\"Neo\"})", "Export the cypher statement results" );
            doc.run();
        }
        server.shutdown();
        db.shutdown();
    }

    @Test
    public void testDumpDatabase() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();
        final GraphDatabaseShellServer server = new GraphDatabaseShellServer( db, false );

        Documenter doc = new Documenter( "database dump", server );
        doc.add( "create index on :Person(name);", "", "create an index" );
        doc.add( "create (m:Person:Hacker {name:'Mattias'}), (m)-[:KNOWS]->(m);", "", "create one labeled node and a relationship" );
        doc.add( "dump", "begin" +
                lineSeparator() + "create index on :`Person`(`name`)" +
                lineSeparator() + "create (_0:`Person`:`Hacker` {`name`:\"Mattias\"})" +
                lineSeparator() + "create _0-[:`KNOWS`]->_0" +
                lineSeparator() + ";" +
                lineSeparator() + "commit", "Export the whole database including indexes" );
        doc.run();
        server.shutdown();
        db.shutdown();
    }

    @Test
    public void testMatrix() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .loadPropertiesFromURL( getClass().getResource( "/autoindex.properties" ) ).newGraphDatabase();
        final GraphDatabaseShellServer server = new GraphDatabaseShellServer( db, false );

        Documenter doc = new Documenter( "a matrix example", server );
        doc.add("mknode --cd", "", "Create a reference node");
        doc.add( "mkrel -t ROOT -c -v", "created",
                "create the Thomas Andersson node" );
        doc.add( "cd 1", "", "go to the new node" );
        doc.add( "set name \"Thomas Andersson\"", "", "set the name property" );
        doc.add( "mkrel -t KNOWS -cv", "", "create Thomas direct friends" );
        doc.add( "cd 2", "", "go to the new node" );
        doc.add( "set name \"Trinity\"", "", "set the name property" );
        doc.add( "cd ..", "", "go back in the history stack" );
        doc.add( "mkrel -t KNOWS -cv", "", "create Thomas direct friends" );
        doc.add( "cd 3", "", "go to the new node" );
        doc.add( "set name \"Morpheus\"", "", "set the name property" );
        doc.add( "mkrel -t KNOWS 2", "", "create relationship to Trinity" );

        doc.add( "ls -rv", "", "list the relationships of node 3" );
        doc.add( "cd -r 2", "", "change the current position to relationship #2" );

        doc.add( "set -t int age 3", "", "set the age property on the relationship" );
        doc.add( "cd ..", "", "back to Morpheus" );
        doc.add( "cd -r 3", "", "next relationship" );
        doc.add( "set -t int age 90", "", "set the age property on the relationship" );
        doc.add( "cd start", "", "position to the start node of the current relationship" );

        doc.add( "", "", "We're now standing on Morpheus node, so let's create the rest of the friends." );
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

        doc.add( "", "", "" );
        doc.add( "start morpheus = node:node_auto_index(name='Morpheus') " +
                "match morpheus-[:KNOWS]-zionist " +
                "return zionist.name;",
                "Trinity",
                "Morpheus' friends, looking up Morpheus by name in the Neo4j autoindex" );
        doc.add( "cypher 2.2 start morpheus = node:node_auto_index(name='Morpheus') " +
                "match morpheus-[:KNOWS]-zionist " +
                "return zionist.name;",
                "Cypher",
                "Morpheus' friends, looking up Morpheus by name in the Neo4j autoindex" );
//        doc.add( "profile start morpheus = node:node_auto_index(name='Morpheus') " +
//                "match morpheus-[:KNOWS]-zionist " +
//                "return zionist.name;",
//                "ColumnFilter",
//                "profile the query by displaying more query execution information" );
        doc.run(); // wrapping this in a tx will cause problems, so we don't
        server.shutdown();

        try (Transaction tx = db.beginTx())
        {
            assertEquals( 7, Iterables.count( GlobalGraphOperations.at( db ).getAllRelationships() ) );
            assertEquals( 7, Iterables.count( GlobalGraphOperations.at( db ).getAllNodes() ) );
            boolean foundRootAndNeoRelationship = false;
            for ( Relationship relationship : GlobalGraphOperations.at( db )
                    .getAllRelationships() )
            {
                if ( relationship.getType().name().equals( "ROOT" ) )
                {
                    foundRootAndNeoRelationship = true;
                    assertFalse( "The root node should not have a name property.", relationship.getStartNode()
                            .hasProperty( "name" ) );
                    assertEquals( "Thomas Andersson", relationship.getEndNode()
                            .getProperty( "name", null ) );
                }
            }
            assertTrue( "Could not find the node connecting the root and Neo nodes.", foundRootAndNeoRelationship );
            tx.success();
        }

        try ( PrintWriter writer = doc.getWriter( "shell-matrix-example-graph" );
                Transaction tx = db.beginTx() )
        {
            writer.println( createGraphViz( "Shell Matrix Example", db, "graph" ) );
            writer.flush();
            tx.success();
        }
        db.shutdown();
    }

    private void assertShellException( final String command ) throws Exception
    {
        try
        {
            this.parse( command );
            fail( "Should fail with " + ShellException.class.getSimpleName() );
        }
        catch ( ShellException e )
        {
            // Good
        }
    }
}
