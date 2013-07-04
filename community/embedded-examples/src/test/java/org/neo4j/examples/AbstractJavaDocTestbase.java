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
package org.neo4j.examples;

import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.JavaTestDocsGenerator;
import org.neo4j.test.TestData;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.visualization.asciidoc.AsciidocHelper;

public class AbstractJavaDocTestbase implements GraphHolder
{
    public @Rule
    TestData<JavaTestDocsGenerator> gen = TestData.producedThrough( JavaTestDocsGenerator.PRODUCER );
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor( this, true ) );
    protected static GraphDatabaseService db;
    protected ExecutionEngine engine;
    
    protected String createCypherSnippet( String cypherQuery )
    {
        String snippet = org.neo4j.cypher.internal.parser.prettifier.Prettifier$.MODULE$.apply( cypherQuery );
        return AsciidocHelper.createAsciiDocSnippet( "cypher", snippet );
    }

    @BeforeClass
    public static void init()
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase();
    }
    
    @AfterClass
    public static void shutdownDb()
    {
        try
        {
            if ( db != null ) db.shutdown();
        }
        finally
        {
            db = null;
        }
    }

    @Before
    public void setUp()
    {
        ((ImpermanentGraphDatabase)db).cleanContent();
        gen.get().setGraph( db );
        engine = new ExecutionEngine( db );
    }

    @After
    public void doc()
    {
        gen.get().document( "target/docs/dev", "examples" );
    }

    @Override
    public GraphDatabaseService graphdb()
    {
        return db;
    }
}
