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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import javax.annotation.Resource;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.SilentLocalOutput;
import org.neo4j.shell.Variables;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( ImpermanentDatabaseExtension.class )
public class CdTest
{
    @Resource
    public ImpermanentDatabaseRule dbRule;

    private final Output silence = new SilentLocalOutput();
    private final Session session = new Session( "test" );
    private GraphDatabaseAPI db;
    private GraphDatabaseShellServer server;

    @BeforeEach
    public void setup() throws Exception
    {
        db = dbRule.getGraphDatabaseAPI();
        server = new GraphDatabaseShellServer( db );
        session.set( Variables.TITLE_KEYS_KEY, "name" );
    }

    @AfterEach
    public void shutdown() throws Exception
    {
        server.shutdown();
    }

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

        assertTrue( found,
                "Should have found a candidate among " + candidates + " starting with '" + shouldStartWith + "'" );
    }

    private Node createNodeWithSomeSubNodes( String... names )
    {
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
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
}
