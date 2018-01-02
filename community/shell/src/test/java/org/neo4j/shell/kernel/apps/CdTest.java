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
package org.neo4j.shell.kernel.apps;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.SilentLocalOutput;
import org.neo4j.shell.Variables;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertTrue;

public class CdTest
{
    @Test
    public void shouldProvideTabCompletions() throws Exception
    {
        // GIVEN
        Node root = createNodeWithSomeSubNodes( "Mattias", "Magnus", "Tobias" );
        Cd app = (Cd) server.findApp( "cd" );
        app.execute( new AppCommandParser( server, "cd -a " + root.getId() ), session, silence );

        // WHEN
        List<String> candidates = app.completionCandidates( "cd Ma", session );

        // THEN
        assertHasCandidate( candidates, "Mattias" );
        assertHasCandidate( candidates, "Magnus" );
    }

    private void assertHasCandidate( List<String> candidates, String shouldStartWith )
    {
        boolean found = false;
        for ( String candidate : candidates )
        {
            if ( candidate.startsWith( shouldStartWith ) )
            {
                found = true;
            }
        }

        assertTrue( "Should have found a candidate among " + candidates + " starting with '" + shouldStartWith + "'",
                found );
    }

    private Node createNodeWithSomeSubNodes( String... names )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            Node root = db.createNode();
            for ( String name : names )
            {
                Node node = db.createNode();
                node.setProperty( "name", name );
                root.createRelationshipTo( node, MyRelTypes.TEST );
            }
            tx.success();
            return root;
        }
    }

    private final Output silence = new SilentLocalOutput();
    private final Session session = new Session( "test" );
    public final @Rule DatabaseRule dbRule = new ImpermanentDatabaseRule();
    private GraphDatabaseAPI db;
    private GraphDatabaseShellServer server;

    @Before
    public void setup() throws Exception
    {
        db = dbRule.getGraphDatabaseAPI();
        server = new GraphDatabaseShellServer( db );
        session.set( Variables.TITLE_KEYS_KEY, "name" );
    }

    @After
    public void shutdown() throws Exception
    {
        server.shutdown();
    }
}
