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

import org.junit.Test;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.test.GraphDescription.Graph;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.visualization.asciidoc.AsciidocHelper.createQueryResultSnippet;

public class AclExampleDocTest extends ImpermanentGraphJavaDocTestBase
{
    private static final String ACL_DOCUMENTATION =
            "This example gives a generic overview of an approach to handling Access Control Lists (ACLs) in graphs,\n" +
            "and a simplified example with concrete queries.\n" +
            "\n" +
            "== Generic approach ==\n" +
            "\n" +
            "In many scenarios, an application needs to handle security on some form of managed\n" +
            "objects. This example describes one pattern to handle this through the use of a graph structure and traversers\n" +
            "that build a full permissions-structure for any managed object with exclude and include overriding\n" +
            "possibilities. This results in a dynamic construction of ACLs based on the\n" +
            "position and context of the managed object.\n" +
            "\n" +
            "The result is a complex security scheme that can easily be implemented in a graph structure,\n" +
            "supporting permissions overriding, principal and content composition, without duplicating data anywhere.\n" +
            "\n" +
            "image::ACL.png[scaledwidth=\"95%\"]\n" +
            "\n" +
            "=== Technique ===\n" +
            "\n" +
            "As seen in the example graph layout, there are some key concepts in this domain model:\n" +
            "\n" +
            "* The managed content (folders and files) that are connected by +HAS_CHILD_CONTENT+ relationships\n" +
            "\n" +
            "* The Principal subtree pointing out principals that can act as ACL members, pointed out by the +PRINCIPAL+ relationships.\n" +
            "\n" +
            "* The aggregation of principals into groups, connected by the +IS_MEMBER_OF+ relationship. One principal (user or group) can be part of many groups at the same time.\n" +
            "\n" +
            "* The +SECURITY+ -- relationships, connecting the content composite structure to the principal composite structure, containing a addition/removal modifier property (\"++RW+\").\n" +
            "\n" +
            "\n" +
            "=== Constructing the ACL ===\n" +
            "\n" +
            "The calculation of the effective permissions (e.g. Read, Write, Execute) for a\n" +
            "principal for any given ACL-managed node (content) follows a number of rules that will be encoded into the permissions-traversal:\n" +
            "\n" +
            "=== Top-down-Traversal ===\n" +
            "\n" +
            "This approach will let you define a generic permission pattern on the root content,\n" +
            "and then refine that for specific sub-content nodes and specific principals.\n" +
            "\n" +
            ". Start at the content node in question traverse upwards to the content root node to determine the path to it.\n" +
            ". Start with a effective optimistic permissions list of \"all permitted\" (+111+ in a bit encoded ReadWriteExecute case)\n" +
            "  or +000+ if you like pessimistic security handling (everything is forbidden unless explicitly allowed).\n" +
            ". Beginning from the topmost content node, look for any +SECURITY+ relationships on it.\n" +
            ". If found, look if the principal in question is part of the end-principal of the +SECURITY+ relationship.\n" +
            ". If yes, add the \"+++\" permission modifiers to the existing permission pattern, revoke the \"+-+\" permission modifiers from the pattern.\n" +
            ". If two principal nodes link to the same content node, first apply the more generic prinipals modifiers.\n" +
            ". Repeat the security modifier search all the way down to the target content node, thus overriding more\n" +
            "  generic permissions with the set on nodes closer to the target node.\n" +
            "\n" +
            "The same algorithm is applicable for the bottom-up approach, basically just\n" +
            "traversing from the target content node upwards and applying the security modifiers dynamically\n" +
            "as the traverser goes up.\n" +
            "\n" +
            "=== Example ===\n" +
            "\n" +
            "Now, to get the resulting access rights for e.g. \"+user 1+\" on the \"+My File.pdf+\" in a Top-Down\n" +
            "approach on the model in the graph above would go like:\n" +
            "\n" +
            ". Traveling upward, we start with \"+Root folder+\", and set the permissions to +11+ initially (only considering Read, Write).\n" +
            ". There are two +SECURITY+ relationships to that folder.\n" +
            "  User 1 is contained in both of them, but \"+root+\" is more generic, so apply it first then \"+All principals+\" `+W` `+R` -> +11+.\n" +
            ". \"+Home+\" has no +SECURITY+ instructions, continue.\n" +
            ". \"+user1 Home+\" has +SECURITY+.\n" +
            "  First apply \"+Regular Users+\" (`-R` `-W`) -> +00+, Then \"+user 1+\" (`+R` `+W`) -> +11+.\n" +
            ". The target node \"+My File.pdf+\" has no +SECURITY+ modifiers on it, so the effective permissions for \"+User 1+\" on \"+My File.pdf+\" are +ReadWrite+ -> +11+.\n" +
            "\n" +
            "== Read-permission example ==\n" +
            "\n" +
            "In this example, we are going to examine a tree structure of +directories+ and\n" +
            "+files+. Also, there are users that own files and roles that can be assigned to\n" +
            "users. Roles can have permissions on directory or files structures (here we model\n" +
            "only +canRead+, as opposed to full +rwx+ Unix permissions) and be nested. A more thorough\n" +
            "example of modeling ACL structures can be found at\n" +
            "http://www.xaprb.com/blog/2006/08/16/how-to-build-role-based-access-control-in-sql/[How to Build Role-Based Access Control in SQL].\n" +
            "\n" +
            "include::ACL-graph.asciidoc[]\n" +
            "\n" +
            "=== Find all files in the directory structure ===\n" +
            "\n" +
            "In order to find all files contained in this structure, we need a variable length\n" +
            "query that follows all +contains+ relationships and retrieves the nodes at the other\n" +
            "end of the +leaf+ relationships.\n" +
            "\n" +
            "@@query1\n" +
            "\n" +
            "resulting in:\n" +
            "\n" +
            "@@result1\n" +
            "\n" +
            "=== What files are owned by whom? ===\n" +
            "\n" +
            "If we introduce the concept of ownership on files, we then can ask for the owners of the files we find --\n" +
            "connected via +owns+ relationships to file nodes.\n" +
            "\n" +
            "@@query2\n" +
            "\n" +
            "Returning the owners of all files below the +FileRoot+ node.\n" +
            "\n" +
            "@@result2\n" +
            "\n" +
            "\n" +
            "=== Who has access to a File? ===\n" +
            "\n" +
            "If we now want to check what users have read access to all Files, and define our ACL as\n" +
            "\n" +
            "* The root directory has no access granted.\n" +
            "* Any user having a role that has been granted +canRead+ access to one of the parent folders of a File has read access.\n" +
            "\n" +
            "In order to find users that can read any part of the parent folder hierarchy above the files,\n" +
            "Cypher provides optional variable length path.\n" +
            "\n" +
            "@@query3\n" +
            "\n" +
            "This will return the +file+, and the directory where the user has the +canRead+ permission along\n" +
            "with the +user+ and their +role+.\n" +
            "\n" +
            "@@result3\n" +
            "\n" +
            "The results listed above contain +null+ for optional path segments, which can be mitigated by either asking several\n" +
            "queries or returning just the really needed values.";

    @Documented( ACL_DOCUMENTATION )
    @Graph( value = {
            "Root has Role",
            "Role subRole SUDOers",
            "Role subRole User",
            "User member User1",
            "User member User2",
            "Root has FileRoot",
            "FileRoot contains Home",
            "Home contains HomeU1",
            "Home contains HomeU2",
            "HomeU1 leaf File1",
            "HomeU2 contains Desktop",
            "Desktop leaf File2",
            "FileRoot contains etc",
            "etc contains init.d",
            "SUDOers member Admin1",
            "SUDOers member Admin2",
            "User1 owns File1",
            "User2 owns File2",
        "SUDOers canRead FileRoot"})
    @Test
    public void ACL_structures_in_graphs()
    {
        data.get();
        // only add the visualization if it's actually used.
        //gen.get().addSnippet( "graph1", createGraphViz("The Domain Structure", graphdb(), gen.get().getTitle()) );

        //Files
        //TODO: can we do open ended?
        String query = "match ({name: 'FileRoot'})-[:contains*0..]->(parentDir)-[:leaf]->(file) return file";
        gen.get().addSnippet( "query1", createCypherSnippet( query ) );
        String result = db.execute( query )
                .resultAsString();
        assertTrue( result.contains("File1") );
        gen.get()
                .addSnippet( "result1", createQueryResultSnippet( result ) );

        //Ownership
        query = "match ({name: 'FileRoot'})-[:contains*0..]->()-[:leaf]->(file)<-[:owns]-(user) return file, user";
        gen.get().addSnippet( "query2", createCypherSnippet( query ) );
        result = db.execute( query )
                .resultAsString();
        assertTrue( result.contains("File1") );
        assertTrue( result.contains("User1") );
        assertTrue( result.contains("User2") );
        assertTrue( result.contains("File2") );
        assertFalse( result.contains("Admin1") );
        assertFalse( result.contains("Admin2") );
        gen.get()
                .addSnippet( "result2", createQueryResultSnippet( result ) );

        //ACL
        query = "MATCH (file)<-[:leaf]-()<-[:contains*0..]-(dir) " +
                "OPTIONAL MATCH (dir)<-[:canRead]-(role)-[:member]->(readUser) " +
                "WHERE file.name =~ 'File.*' " +
                "RETURN file.name, dir.name, role.name, readUser.name";
        gen.get().addSnippet( "query3", createCypherSnippet( query ) );
        result = db.execute( query )
                .resultAsString();
        assertTrue( result.contains("File1") );
        assertTrue( result.contains("File2") );
        assertTrue( result.contains("Admin1") );
        assertTrue( result.contains("Admin2") );
        gen.get()
                .addSnippet( "result3", createQueryResultSnippet( result ) );
    }
}
