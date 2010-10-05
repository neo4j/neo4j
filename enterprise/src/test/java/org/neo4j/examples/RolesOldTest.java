/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

 
package org.neo4j.examples;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class RolesOldTest
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
            Traverser traverser = admins.traverse(
                    Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH,
                    ReturnableEvaluator.ALL_BUT_START_NODE, RoleRels.PART_OF,
                    Direction.INCOMING, RoleRels.MEMBER_OF, Direction.INCOMING );
            for ( Node part : traverser )
            {
                System.out.println( part.getProperty( NAME )
                                    + " "
                                    + ( traverser.currentPosition().depth() - 1 ) );
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
            Traverser traverser = jale.traverse( Traverser.Order.DEPTH_FIRST,
                    StopEvaluator.END_OF_GRAPH,
                    ReturnableEvaluator.ALL_BUT_START_NODE, RoleRels.MEMBER_OF,
                    Direction.OUTGOING, RoleRels.PART_OF, Direction.OUTGOING );
            for ( Node membership : traverser )
            {
                System.out.println( membership.getProperty( NAME )
                                    + " "
                                    + ( traverser.currentPosition().depth() - 1 ) );
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
            Traverser traverser = referenceNode.traverse(
                    Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH,
                    ReturnableEvaluator.ALL_BUT_START_NODE, RoleRels.ROOT,
                    Direction.INCOMING, RoleRels.PART_OF, Direction.INCOMING );
            for ( Node group : traverser )
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
            Traverser traverser = referenceNode.traverse(
                    Traverser.Order.BREADTH_FIRST, StopEvaluator.END_OF_GRAPH,
                    new ReturnableEvaluator()
                    {
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
                    }, RoleRels.ROOT,
                    Direction.INCOMING, RoleRels.PART_OF, Direction.INCOMING,
                    RoleRels.MEMBER_OF, Direction.INCOMING );
            for ( Node group : traverser )
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
