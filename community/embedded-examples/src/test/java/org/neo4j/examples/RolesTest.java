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

import org.junit.Test;
import org.neo4j.graphdb.*;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription.Graph;

import static org.junit.Assert.assertTrue;
import static org.neo4j.test.AsciiDocGenerator.createSourceSnippet;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.*;

public class RolesTest extends AbstractJavaDocTestbase
{
    private static final String NAME = "name";

    public enum RoleRels implements RelationshipType
    {
        ROOT,
        PART_OF,
        MEMBER_OF;
    }

    /**
     * This is an example showing a hierarchy of 
     * roles. 
     * What's interesting is that a tree is not sufficient for storing this structure,
     * as elaborated below.
     * 
     * image::roles.png[scaledwidth="100%"]
     * 
     * This is an implementation of an example found in the article 
     * http://www.codeproject.com/KB/database/Modeling_DAGs_on_SQL_DBs.aspx[A Model to Represent Directed Acyclic Graphs (DAG) on SQL Databases] 
     * by http://www.codeproject.com/script/Articles/MemberArticles.aspx?amid=274518[Kemal Erdogan].
     * The article discusses how to store http://en.wikipedia.org/wiki/Directed_acyclic_graph[
     * directed acyclic graphs] (DAGs) 
     * in SQL based DBs. DAGs are almost trees, but with a twist: it may be possible to reach 
     * the same node through different paths. Trees are restricted from this possibility, which 
     * makes them much easier to handle. In our case it is "Ali" and "Engin" that brake 
     * the tree pattern, as they are both admins and users. Reality often looks this way and 
     * can't be captured by tree structures.
     * 
     * In the article an SQL + Stored Procedure solution is provided. The main idea, 
     * that also have some support from scientists, is to pre-calculate all possible (transitive) paths. 
     * Pros and cons of this approach:
     * 
     * * decent performance on read
     * * low performance on insert
     * * wastes _lots_ of space
     * * relies on stored procedures
     * 
     * In Neo4j storing the roles is trivial. In this case we use +PART_OF+ (blue edges) relationships 
     * to model the group hierarchy and +MEMBER_OF+ (green edges) to model membership in groups. 
     * We also connect the top level groups to the reference node by +ROOT+ relationships. 
     * This gives us a useful partitioning of the graph. Neo4j has no predefined relationship 
     * types, you are free to create any relationship types and give them any semantics you want.
     * 
     * Lets now have a look at how to retrieve information from the graph. The Java code is using
     * the Neo4j Traversal API (<<graphdb-neo4j-traversal>>), the queries are done using the <<cypher-query-lang>>.
     *
     * == Get the admins ==
     * 
     * @@get-admins
     * 
     * resulting in the output
     * 
     * @@o-get-admins
     * 
     * As Cypher, this looks like:
     * 
     * @@query-get-admins
     * 
     * resulting in:
     * 
     * @@o-query-get-admins
     *
     * == Get the group memberships of a user ==
     * 
     * Using the <<graphdb-neo4j-traversal,Neo4j Java Traversal API>>, this query looks like:
     * 
     * @@get-user-memberships
     * 
     * resuling in: 
     * 
     * @@o-get-user-memberships
     *
     * In Cypher:
     * 
     * @@query-get-user-memberships
     * 
     * @@o-query-get-user-memberships
     * 
     * == Get all groups ==
     * 
     * In Java: 
     * 
     * @@get-groups
     * 
     * resulting in:
     * 
     * @@o-get-groups
     * 
     * In Cypher:
     * 
     * @@query-get-groups
     * 
     * @@o-query-get-groups
     * 
     * == Get all members of all groups ==
     * 
     * Now, let's try to find all users in the system being part of any group.
     * 
     * in Java:
     * 
     * @@get-members
     * 
     * @@o-get-members
     * 
     * In Cypher, this looks like:
     * 
     * @@query-get-members
     * 
     * and results in the following output, listing even duplicate pathes to users,
     * see e.g. user +Engin+.
     * 
     * @@o-query-get-members
     * 
     * As seen above, querying even more complex scenarios can be done using comparatively short
     * constructs in Java and other query mechanisms.
     *
     * Full source code: https://github.com/neo4j/community/blob/{neo4j-git-tag}/embedded-examples/src/test/java/org/neo4j/examples/RolesTest.java[RolesTest.java]
     */
    @Test
    @Documented
    @Graph( { "Admins ROOT Reference_Node", "Users ROOT Reference_Node",
            "HelpDesk PART_OF Admins", "Managers PART_OF Users",
            "Technicians PART_OF Users", "ABCTechnicians PART_OF Technicians",
            "Ali MEMBER_OF Users", "Ali MEMBER_OF Admins",
            "Engin MEMBER_OF Users", "Engin MEMBER_OF HelpDesk",
            "Demet MEMBER_OF HelpDesk", "Burcu MEMBER_OF Users",
            "Can MEMBER_OF Users", "Gul MEMBER_OF Managers",
            "Fuat MEMBER_OF Managers", "Hakan MEMBER_OF Technicians",
            "Irmak MEMBER_OF Technicians", "Jale MEMBER_OF ABCTechnicians"
    } )
    public void user_roles_in_graphs()
    {
        // get Admins
        gen.get().addSnippet( "get-admins", createSourceSnippet("get-admins", this.getClass()) );
        System.out.println( "All admins:" );
        // START SNIPPET: get-admins
        Node admins = getNodeByName( "Admins" );
        Traverser traverser = admins.traverse( Traverser.Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, RoleRels.PART_OF,
                Direction.INCOMING, RoleRels.MEMBER_OF, Direction.INCOMING );
        // END SNIPPET: get-admins
        
        gen.get().addSnippet( "o-get-admins", createOutputSnippet( traverserToString( traverser ) ) );
        String query = "start admins=node("+ admins.getId() +") match admins<-[:PART_OF*0..]-subrole<-[:MEMBER_OF]-user return user, subrole";
        gen.get().addSnippet( "query-get-admins", createCypherSnippet( query ) );
        String result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("Engin") );
        gen.get().addSnippet( "o-query-get-admins", createQueryResultSnippet( result ) );
        
        //Jale's memberships
        gen.get().addSnippet( "get-user-memberships", createSourceSnippet("get-user-memberships", this.getClass()) );
        // START SNIPPET: get-user-memberships
        Node jale = getNodeByName( "Jale" );
        traverser = jale.traverse( Traverser.Order.DEPTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, RoleRels.MEMBER_OF,
                Direction.OUTGOING, RoleRels.PART_OF, Direction.OUTGOING );
        // END SNIPPET: get-user-memberships
        gen.get().addSnippet( "o-get-user-memberships", createOutputSnippet( traverserToString( traverser ) ) );
        query = "start jale=node("+ jale.getId() +") match jale-[:MEMBER_OF]->()-[:PART_OF*0..]->role return jale, role";
        gen.get().addSnippet( "query-get-user-memberships", createCypherSnippet( query ) );
        result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("Users") );
        gen.get()
                .addSnippet( "o-query-get-user-memberships",
                        createQueryResultSnippet( result ) );
        
        // get all groups
        gen.get().addSnippet( "get-groups", createSourceSnippet("get-groups", this.getClass()) );
        // START SNIPPET: get-groups
        Node referenceNode = getNodeByName( "Reference_Node") ;
        traverser = referenceNode.traverse(
                Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, RoleRels.ROOT,
                Direction.INCOMING, RoleRels.PART_OF, Direction.INCOMING );
        // END SNIPPET: get-groups
        gen.get().addSnippet( "o-get-groups", createOutputSnippet( traverserToString( traverser ) ) );
        query = "start refNode=node("+ referenceNode.getId() +") match refNode<-[:ROOT]->()<-[:PART_OF*0..]-group return group";
        gen.get().addSnippet( "query-get-groups", createCypherSnippet( query ) );
        result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("Users") );
        gen.get()
                .addSnippet( "o-query-get-groups",
                        createQueryResultSnippet( result ) );
        
        //get all members
        gen.get().addSnippet( "get-members", createSourceSnippet("get-members", this.getClass()) );
        // START SNIPPET: get-members
        traverser = referenceNode.traverse(
                Traverser.Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
                new ReturnableEvaluator()
                {
                    @Override
                    public boolean isReturnableNode(
                            TraversalPosition currentPos )
                    {
                        if ( currentPos.isStartNode() )
                        {
                            return false;
                        }
                        Relationship rel = currentPos.lastRelationshipTraversed();
                        return rel.isType( RoleRels.MEMBER_OF );
                    }
                }, RoleRels.ROOT, Direction.INCOMING, RoleRels.PART_OF,
                Direction.INCOMING, RoleRels.MEMBER_OF, Direction.INCOMING );
        // END SNIPPET: get-members
        gen.get().addSnippet( "o-get-members", createOutputSnippet( traverserToString( traverser ) ) );
        query = "start refNode=node("+ referenceNode.getId() +") " +
        		"match p=refNode<-[:ROOT]->parent<-[:PART_OF*0..]-group, group<-[:MEMBER_OF]-user return group.name, user.name, LENGTH(p) " +
        		"order by LENGTH(p)";
        gen.get().addSnippet( "query-get-members", createCypherSnippet( query ) );
        result = engine.execute( parser.parse( query ) ).toString();
        assertTrue( result.contains("Engin") );
        gen.get()
                .addSnippet( "o-query-get-members",
                        createQueryResultSnippet( result ) );
        
    }

    private String traverserToString( Traverser traverser )
    {
        String output = "";
        //we should get two paths back, through Pet1 and Pet3
        for ( Node part : traverser )
        {
            output +=  "Found: " + part.getProperty( NAME ) + " at depth: " + ( traverser.currentPosition().depth() - 1 ) + "\n";
        }
        
        return output;
    }

    private Node getNodeByName( String string )
    {
        return data.get().get( string );
    }
}
