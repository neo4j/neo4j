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
package org.neo4j.cypher.example;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.AsciiDocGenerator;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.JavaTestDocsGenerator;
import org.neo4j.test.TestData;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import static org.neo4j.test.GraphDatabaseServiceCleaner.cleanDatabaseContent;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createCypherSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createQueryResultSnippet;

public class IntroDocTest implements GraphHolder
{
    private static final String DOCS_TARGET = "target/docs/dev/general/";
    public @Rule
    TestData<JavaTestDocsGenerator> gen = TestData.producedThrough( JavaTestDocsGenerator.PRODUCER );
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
    private static GraphDatabaseService graphdb;

    @Test
    @Graph( value = { "John friend Sara", "John friend Joe",
            "Sara friend Maria", "Joe friend Steve" }, autoIndexNodes = true )
    public void intro_examples() throws Exception
    {
        try ( Transaction ignored = graphdb.beginTx() )
        {
            Writer fw = AsciiDocGenerator.getFW( DOCS_TARGET, gen.get().getTitle() );
            data.get();
            fw.append( "\nImagine an example graph like the following one:\n\n" );
            fw.append( AsciiDocGenerator.dumpToSeparateFileWithType( new File( DOCS_TARGET ), "intro.graph",
                    AsciidocHelper.createGraphViz( "Example Graph",
                            graphdb(), "cypher-intro" ) ) );

            fw.append( "\nFor example, here is a query which finds a user called John and John's friends (though not " +
                    "his direct friends) before returning both John and any friends-of-friends that are found." );
            fw.append( "\n\n" );
            String query = "MATCH (john {name: 'John'})-[:friend]->()-[:friend]->(fof) RETURN john.name, fof.name ";
            fw.append( AsciiDocGenerator.dumpToSeparateFileWithType( new File( DOCS_TARGET ), "intro.query",
                    createCypherSnippet( query ) ) );
            fw.append( "\nResulting in:\n\n" );
            fw.append( AsciiDocGenerator.dumpToSeparateFileWithType( new File( DOCS_TARGET ), "intro.result",
                    createQueryResultSnippet( graphdb.execute( query ).resultAsString() ) ) );

            fw.append( "\nNext up we will add filtering to set more parts "
                    + "in motion:\n\nWe take a list of user names "
                    + "and find all nodes with names from this list, match their friends and return "
                    + "only those followed users who have a +name+ property starting with +S+." );
            query = "MATCH (user)-[:friend]->(follower) WHERE "
                    + "user.name IN ['Joe', 'John', 'Sara', 'Maria', 'Steve'] AND follower.name =~ 'S.*' "
                            + "RETURN user.name, follower.name ";
            fw.append( "\n\n" );
            fw.append( AsciiDocGenerator.dumpToSeparateFileWithType( new File( DOCS_TARGET ), "intro.query",
                    createCypherSnippet( query ) ) );
            fw.append( "\nResulting in:\n\n" );
            fw.append( AsciiDocGenerator.dumpToSeparateFileWithType( new File( DOCS_TARGET ), "intro.result",
                    createQueryResultSnippet( graphdb.execute( query ).resultAsString() ) ) );
            fw.close();
        }
    }

    @BeforeClass
    public static void setup() throws IOException
    {
        graphdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
        cleanDatabaseContent( graphdb );
    }
    
    @AfterClass
    public static void shutdown()
    {
        try 
        {
            if ( graphdb != null ) graphdb.shutdown();
        }
        finally
        {
            graphdb = null;
        }
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return graphdb;
    }
}
