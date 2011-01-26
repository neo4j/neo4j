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

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Predicate;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;

public class RolesTest
{
    private static final String GROUP = "group";
    private static final String USER = "user";
    private static final String NAME = "name";

    public enum RoleRels implements RelationshipType
    {
        ROOT,
        PART_OF,
        MEMBER_OF;
    }

    private static final String ROLES_DB = "target/roles-db";
    private static GraphDatabaseService graphDb;
    private static IndexService index;

    @BeforeClass
    public static void setUp()
    {
        deleteFileOrDirectory( new File( ROLES_DB ) );
        graphDb = new EmbeddedGraphDatabase( ROLES_DB );
        index = new LuceneIndexService( graphDb );
        registerShutdownHook();
        createNodespace();
    }

    @AfterClass
    public static void tearDown()
    {
        index.shutdown();
        graphDb.shutdown();
    }

    private static void createNodespace()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            // add the top level groups
            Node admins = createTopLevelGroup( "Admins" );
            Node users = createTopLevelGroup( "Users" );

            // add other groups
            Node helpDesk = createGroup( "HelpDesk", admins );
            Node managers = createGroup( "Managers", users );
            Node technicians = createGroup( "Technicians", users );
            Node abcTechnicians = createGroup( "ABCTechnicians", technicians );

            // add the users
            createUser( "Ali", admins, users );
            createUser( "Burcu", users );
            createUser( "Can", users );
            createUser( "Demet", helpDesk );
            createUser( "Engin", helpDesk, users );
            createUser( "Fuat", managers );
            createUser( "Gul", managers );
            createUser( "Hakan", technicians );
            createUser( "Irmak", technicians );
            createUser( "Jale", abcTechnicians );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public static void createRoles()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private static Node createTopLevelGroup( final String name )
    {
        return createNode( name, RoleRels.ROOT, GROUP,
                graphDb.getReferenceNode() );
    }

    private static Node createGroup( final String name,
            final Node... containedIn )
    {
        return createNode( name, RoleRels.PART_OF, GROUP, containedIn );
    }

    private static Node createUser( final String name,
            final Node... containedIn )
    {
        return createNode( name, RoleRels.MEMBER_OF, USER, containedIn );
    }

    private static Node createNode( final String name,
            final RelationshipType relType, final String category,
            final Node... containedIn )
    {
        Node node = graphDb.createNode();
        node.setProperty( NAME, name );
        index.index( node, category, name );
        for ( Node parent : containedIn )
        {
            node.createRelationshipTo( parent, relType );
        }
        return node;
    }

    private static Node getUserByName( final String name )
    {
        return getNodeByName( USER, name );
    }

    private static Node getGroupByName( String name )
    {
        return getNodeByName( GROUP, name );
    }

    private static Node getNodeByName( final String category, final String name )
    {
        return index.getSingleNode( category, name );
    }

    @Test
    public void getAllAdmins()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            System.out.println( "All admins:" );
            // START SNIPPET: get-admins
            Node admins = getGroupByName( "Admins" );
            TraversalDescription td = Traversal.description().breadthFirst().relationships(
                    RoleRels.PART_OF, Direction.INCOMING ).relationships(
                    RoleRels.MEMBER_OF, Direction.INCOMING ).filter(
                    Traversal.returnAllButStartNode() );
            for ( Path path : td.traverse( admins ) )
            {
                Node part = path.endNode();
                System.out.println( part.getProperty( NAME )
                                    + " "
                                    + ( path.length() - 1 ) );
            }
            // END SNIPPET: get-admins
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void getJalesMemberships() throws Exception
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            System.out.println( "Jale's memberships:" );
            // START SNIPPET: get-user-memberships
            Node jale = getUserByName( "Jale" );
            TraversalDescription td = Traversal.description().depthFirst().relationships(
                    RoleRels.MEMBER_OF, Direction.OUTGOING ).relationships(
                    RoleRels.PART_OF, Direction.OUTGOING ).filter(
                    Traversal.returnAllButStartNode() );
            for ( Path path : td.traverse( jale ) )
            {
                Node membership = path.endNode();
                System.out.println( membership.getProperty( NAME )
                                    + " "
                                    + ( path.length() - 1 ) );
            }
            // END SNIPPET: get-user-memberships
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void getAllGroups() throws Exception
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            System.out.println( "All groups:" );
            // START SNIPPET: get-groups
            Node referenceNode = graphDb.getReferenceNode();
            TraversalDescription td = Traversal.description().breadthFirst().relationships(
                    RoleRels.ROOT, Direction.INCOMING ).relationships(
                    RoleRels.PART_OF, Direction.INCOMING ).filter(
                    Traversal.returnAllButStartNode() );
            for ( Node group : td.traverse( referenceNode ).nodes() )
            {
                System.out.println( group.getProperty( NAME ) );
            }
            // END SNIPPET: get-groups
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void getAllMembers() throws Exception
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            System.out.println( "All members:" );
            // START SNIPPET: get-members
            Node referenceNode = graphDb.getReferenceNode();
            TraversalDescription td = Traversal.description().breadthFirst().relationships(
                    RoleRels.ROOT, Direction.INCOMING ).relationships(
                    RoleRels.MEMBER_OF, Direction.INCOMING ).relationships(
                    RoleRels.PART_OF, Direction.INCOMING ).filter(
                    new Predicate<Path>()
                    {
                        public boolean accept( Path item )
                        {
                            if ( item.length() == 0 )
                            {
                                return false;
                            }
                            return item.lastRelationship().isType(
                                    RoleRels.MEMBER_OF );
                        }
                    } );
            for ( Node group : td.traverse( referenceNode ).nodes() )
            {
                System.out.println( group.getProperty( NAME ) );
            }
            // END SNIPPET: get-members
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private static void registerShutdownHook()
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                index.shutdown();
                graphDb.shutdown();
            }
        } );
    }

    private static void deleteFileOrDirectory( final File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }
}
