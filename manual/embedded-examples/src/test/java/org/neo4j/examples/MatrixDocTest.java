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
package org.neo4j.examples;

import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.test.JavaDocsGenerator;

import static org.junit.Assert.assertTrue;

import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

public class MatrixDocTest
{
    private static JavaDocsGenerator gen;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        gen = new JavaDocsGenerator( "matrix-traversal-java", "dev" );
    }

    @Test
    public void newMatrix() throws Exception
    {
        NewMatrix newMatrix = new NewMatrix();
        newMatrix.setUp();
        String friends = newMatrix.printNeoFriends();
        String hackers = newMatrix.printMatrixHackers();
        newMatrix.shutdown();
        check( friends, hackers );
        gen.saveToFile( "new-friends", createOutputSnippet( friends ) );
        gen.saveToFile( "new-hackers", createOutputSnippet( hackers ) );
    }

    private void check( String friends, String hackers )
    {
        assertTrue( friends.contains( "friends found: 4" ) );
        assertTrue( friends.contains( "Trinity" ) );
        assertTrue( friends.contains( "Morpheus" ) );
        assertTrue( friends.contains( "Cypher" ) );
        assertTrue( friends.contains( "Agent Smith" ) );
        assertTrue( hackers.contains( "hackers found: 1" ) );
        assertTrue( hackers.contains( "The Architect" ) );
    }
}
