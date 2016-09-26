/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.orderedpath;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.doc.tools.JavaDocsGenerator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

public class OrderedPathDocTest
{
    private static OrderedPath orderedPath;
    private static JavaDocsGenerator gen;
    private static GraphDatabaseService db;

    @BeforeClass
    public static void setUp() throws IOException
    {
        if ( OrderedPath.DB_PATH.exists() )
        {
            FileUtils.deleteRecursively( OrderedPath.DB_PATH );
        }
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        orderedPath = new OrderedPath( db );
        gen = new JavaDocsGenerator( "ordered-path-java", "dev" );
    }

    @AfterClass
    public static void tearDown()
    {
        orderedPath.shutdownGraph();
    }

    @Test
    public void testPath()
    {
        Node A = orderedPath.createTheGraph();
        TraversalDescription traversalDescription = orderedPath.findPaths();
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, count( traversalDescription.traverse( A ) ) );
        }
        String output = orderedPath.printPaths( traversalDescription, A );
        assertTrue( output.contains( "(A)--[REL1]-->(B)--[REL2]-->(C)--[REL3]-->(D)" ) );
        String graph = AsciidocHelper.createGraphVizDeletingReferenceNode(
                "Ordered Path Graph", orderedPath.db, "java" );
        assertFalse( graph.isEmpty() );
        gen.saveToFile( "graph", graph );
        gen.saveToFile( "output", createOutputSnippet( output ) );
    }
}
