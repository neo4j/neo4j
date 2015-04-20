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

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.neo4j.test.JavaDocsGenerator;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

import static org.junit.Assert.assertFalse;

public class EmbeddedNeo4jDocTest
{
    private static EmbeddedNeo4j hello;
    private static JavaDocsGenerator gen;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception
    {
        hello = new EmbeddedNeo4j();
        gen = new JavaDocsGenerator( "hello-world-java", "dev" );
    }

    @Test
    public void test() throws IOException
    {
        hello.createDb();
        String graph = AsciidocHelper.createGraphVizDeletingReferenceNode(
                "Hello World Graph",
                hello.graphDb, "java" );
        assertFalse( graph.isEmpty() );
        gen.saveToFile( "graph", graph );

        assertFalse( hello.greeting.isEmpty() );
        gen.saveToFile( "output", hello.greeting + "\n\n" );

        hello.removeData();
        hello.shutDown();
    }
}
