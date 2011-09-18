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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.AsciiDocGenerator.createSourceSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createCypherSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createGraphViz;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createOutputSnippet;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.cypher.javacompat.CypherParser;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription;
import org.neo4j.test.GraphDescription.Graph;
import org.neo4j.test.GraphHolder;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.JavaTestDocsGenerator;
import org.neo4j.test.TestData;

public class ShortDocumentationExamplesTest implements GraphHolder
{
    public @Rule
    TestData<JavaTestDocsGenerator> gen = TestData.producedThrough( JavaTestDocsGenerator.PRODUCER );
    public @Rule
    TestData<Map<String, Node>> data = TestData.producedThrough( GraphDescription.createGraphFor(
            this, true ) );
 
    /**
     * Uniqueness of Paths in traversals.
     * 
     * This example is demonstrating the use of node uniqueness.
     * Below an imaginary domain graph with Principals
     * that own pets that are descendant to other pets.
     * 
     * @@graph
     * 
     * In order to return which all descendants 
     * of +Pet0+ which have the relation +owns+ to Principal1 (+Pet1+ and +Pet3+),
     * the Uniqueness of the traversal needs to be set to 
     * +NODE_PATH+ rather than the default +NODE_GLOBAL+ so that nodes
     * can be traversed more that once, and paths that have
     * different nodes but can have some nodes in common (like the
     * start and end node) can be returned.
     * 
     * @@traverser
     * 
     * This will return the following paths:
     * 
     * @@output
     */
    @Graph({"Pet0 descendant Pet1",
        "Pet0 descendant Pet2",
        "Pet0 descendant Pet3",
        "Principal1 owns Pet1",
        "Principal2 owns Pet2",
        "Principal1 owns Pet3"})
    @Test
    @Documented
    public void pathUniquenesExample()
    {
        Node start = data.get().get( "Pet0" );
        gen.get().addSnippet( "graph", createGraphViz("descendants1", graphdb(), gen.get().getTitle()) );
        String tagName = "traverser";
        gen.get();
        gen.get().addSnippet( tagName, createSourceSnippet(tagName, this.getClass()) );
        // START SNIPPET: traverser
        final Node target = data.get().get( "Principal1" );
        TraversalDescription td = Traversal.description().uniqueness(Uniqueness.NODE_PATH ).evaluator( new Evaluator()
        {
            @Override
            public Evaluation evaluate( Path path )
            {
                if(path.endNode().equals( target )) {
                    return Evaluation.INCLUDE_AND_PRUNE;
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        } );
        
        Traverser results = td.traverse( start );
        // END SNIPPET: traverser
        String output = "";
        int count = 0;
        //we should get two paths back, through Pet1 and Pet3
        for(Path path : results)
        {       
            count++;
            output += path.toString()+"\n";
        }
        gen.get().addSnippet( "output", createOutputSnippet(output) );
        assertEquals(2, count);
    }
    
        /**
     * In this example, we are going to examine a tree structure of +directories+ and
     * +files+. Also, there are users that own files and roles that can be assigned to
     * users. Roles can have permissions on directory or files structures (here we model
     * only canRead, as opposed to full +rwx+ Unix permissions) and be nested. A more thorough
     * example of modeling ACL structures can be found at 
     * http://www.xaprb.com/blog/2006/08/16/how-to-build-role-based-access-control-in-sql/[How to Build Role-Based Access Control in SQL]
     * 
     * @@graph1
     * 
     * == Find all files in the directory structure ==
     * 
     * In order to find all files contained in this structure, we need a variable length
     * query that follows all +contains+ relationships and retrieves the nodes at the other
     * end of the +leaf+ relationships
     * 
     *
     * @@query1
     * 
     * resulting in
     * 
     * @@result1
     * 
     * == What files are owned by whom? ==
     * 
     * If we introduce the concept of ownership on files, we then can ask for files owned by
     * +User1+
     * 
     * @@query2
     * 
     * Resulting in 
     * 
     * @@result2
     * 
     *
     * == Who has access to a File? ==
     * 
     * If we now want to check what users have read access to all Files, and define our ACL as
     * 
     * - the root directory has no access granted
     * - The owning user of a File has read access
     * - any user having a role that has been granted read access to one of the parent folders of the Item has read access.
     * 
     * In order to find users that can read any part of the parent folder hierarchy above the files,
     * Cypher provides optional relationships that make certain subgraphs of the matching pattern optional.
     * 
     * @@query3
     * 
     * @@result3
     * 
     */
    @Documented
    @Graph(autoIndexNodes=true, value = {
            "Root has Role",
            "Root has FileRoot",
            "AdminUser3 hasRole User",
            "User1 hasRole User",
            "User2 hasRole User",
            "User isA Role",
            "Admins isA Role",
            "Admin1 subRoleOf Admins",
            "Admin2 subRoleOf Admins",
            "AdminUser3 hasRole Admins",
            "AdminUser4 hasRole Admins",
//            "User2 hasRole Admin2",
            "User1 hasRole Admin1",
            "User1 owns File1",
            "User2 owns File2",
        "Dir2 leaf File1", 
        "Admins canRead Dir0", 
        "Admin1 canRead Dir2", 
        "Admin2 canRead Dir3", 
        "FileRoot contains Dir0", 
        "Dir1 contains Dir2", 
        "Dir0 contains Dir3", 
        "Dir0 contains Dir1",
        "Dir3 leaf File2"})
    @Test
    public void file_trees_and_graphs()
    {
        data.get();
        gen.get().addSnippet( "graph1", createGraphViz("The Domain Structure", graphdb(), gen.get().getTitle()) );
        CypherParser parser = new CypherParser();
        ExecutionEngine engine = new ExecutionEngine(db);
        
        //Files
        //TODO: can we do open ended?
        String query = "start root=(node_auto_index,'name:Dir0') match (root)-[:contains^0..10]->()-[:leaf]->(file) return file";
        gen.get().addSnippet( "query1", createCypherSnippet( query ) );
        String result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("File1") );
        gen.get().addSnippet( "result1", createOutputSnippet( result ) );
        
        //Ownership
        query = "start root=(node_auto_index,'name:Dir0') match (root)-[:contains^0..10]->()-[:leaf]->(file)<-[:owns]-(user) where user.name = 'User1' return file, user";
        gen.get().addSnippet( "query2", createCypherSnippet( query ) );
        result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("File1") );
        assertFalse( result.contains("File2") );
        gen.get().addSnippet( "result2", createOutputSnippet( result ) );
        
        //ACL
        query = "START file=(node_auto_index,'name:File*') " +
        		"MATCH " +
        		"file<-[:leaf]-dir, " +
        		"file<-[:owns]-owner, " +
        		"path = dir<-[:contains^1..10]-parent," +
        		"parent<-[?:canRead]-role2<-[:hasRole]-readUserMoreThan1DirUp, " +
                "dir<-[?:canRead]-role1<-[:hasRole]-readUser1DirUp " +
        		//TODO: would like to get results the order I specify
        		"RETURN path, file, role1, readUser1DirUp, role2, readUserMoreThan1DirUp, owner";
        gen.get().addSnippet( "query3", createCypherSnippet( query ) );
        result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("File1") );
        assertTrue( result.contains("File2") );
        assertTrue( result.contains("AdminUser3") );
        assertTrue( result.contains("AdminUser4") );
        gen.get().addSnippet( "result3", createOutputSnippet( result ) );
        
        
    }

    private static ImpermanentGraphDatabase db;
    @BeforeClass
    public static void init()
    {
        db = new ImpermanentGraphDatabase("target/descendants");
    }
    
    @Before
    public void setUp() {
        db.cleanContent();
        gen.get().setGraph( db );
    }
    @After
    public void doc() {
        gen.get().document("target/docs","examples");
    }
    @Override
    public GraphDatabaseService graphdb()
    {
        return db;
    }

}
