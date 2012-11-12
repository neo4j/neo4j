/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

import java.io.File;
import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.JavaDocsGenerator;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class OrderedPathTest
{
    private static OrderedPath orderedPath;
    private static JavaDocsGenerator gen;

    @BeforeClass
    public static void setUp() throws IOException
    {
        File dir = new File( OrderedPath.DB_PATH );
        if ( dir.exists() )
        {
            FileUtils.deleteRecursively( dir );
        }
        orderedPath = new OrderedPath( new ImpermanentGraphDatabase() );
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
        assertEquals( 1, count( traversalDescription.traverse( A ) ) );
        String output = orderedPath.printPaths( traversalDescription, A );
        System.out.println( output );
        assertTrue( output.contains( "(A)--[REL1]-->(B)--[REL2]-->(C)--[REL3]-->(D)" ) );
        String graph = AsciidocHelper.createGraphVizDeletingReferenceNode(
                "Ordered Path Graph", orderedPath.db, "java" );
        assertFalse( graph.isEmpty() );
        gen.saveToFile( "graph", graph );
        gen.saveToFile( "output", createOutputSnippet( output ) );
    }
}
