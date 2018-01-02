/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.traversal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.MyRelTypes;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Traverser.Order.BREADTH_FIRST;
import static org.neo4j.graphdb.Traverser.Order.DEPTH_FIRST;

public class TestTraversal extends AbstractNeo4jTestCase
{
    // Tests the traverser factory for sanity checks with corrupted input
    @Test
    public void testSanityChecks1() throws Exception
    {
        // Valid data
        Node root = getGraphDb().createNode();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST, };
        // Null traversable relationships
        this.sanityCheckTraverser( "Sanity check failed: null traversable "
            + "rels should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, null, Direction.OUTGOING,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL );

        // Null stop evaluator
        this.sanityCheckTraverser( "Sanity check failed: null stop eval "
            + "should throw an IllegalArgumentException", BREADTH_FIRST, root,
            traversableRels[0], Direction.OUTGOING, null,
            ReturnableEvaluator.ALL );

        // Null returnable evaluator
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], Direction.OUTGOING,
            StopEvaluator.END_OF_GRAPH, null );
        root.delete();
    }

    @Test
    public void testSanityChecks2() throws Exception
    {
        // ------------- with traverser direction -------------
        // Valid data
        Node root = getGraphDb().createNode();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST, };
        Direction[] traversableDirs = new Direction[] { Direction.OUTGOING };

        // Null traversable relationships
        this.sanityCheckTraverser( "Sanity check failed: null traversable "
            + "rels should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, null, traversableDirs[0],
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL );

        // Null traversable directions
        this.sanityCheckTraverser( "Sanity check failed: null traversable "
            + "rels should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], null,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL );

        // Null stop evaluator
        this.sanityCheckTraverser( "Sanity check failed: null stop eval "
            + "should throw an IllegalArgumentException", BREADTH_FIRST, root,
            traversableRels[0], traversableDirs[0], null,
            ReturnableEvaluator.ALL );

        // Null returnable evaluator
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], traversableDirs[0],
            StopEvaluator.END_OF_GRAPH, null );
        // traversable relationships length not equal to traversable directions
        // length
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], null,
            StopEvaluator.END_OF_GRAPH, null );
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, null, traversableDirs[0],
            StopEvaluator.END_OF_GRAPH, null );
        root.delete();
    }

    // Tests the traverser factory for simple corrupted (null) input, used
    // by testSanityChecks()
    private void sanityCheckTraverser( String failMessage, Order type,
        Node startNode, RelationshipType traversableRel, Direction direction,
        StopEvaluator stopEval, ReturnableEvaluator retEval )
    {
        try
        {
            startNode.traverse( type, stopEval, retEval, traversableRel,
                direction );
            fail( failMessage );
        }
        catch ( IllegalArgumentException iae )
        {
            // This is ok
        }
    }

    // Traverses the full test "ise-tree-like" population breadth first
    // and verifies that it is returned in correct order
    @Test
    public void testBruteBreadthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL };
        Traverser traverser = root.traverse( BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH, traversableRels[1],
            Direction.BOTH );

        try
        {
            this.assertLevelsOfNodes( traverser, new String[][] {
                    new String[] { "1" },
                    new String[] { "3", "4", "2" },
                    new String[] { "7", "8", "9", "6", "5" },
                    new String[] { "14", "12", "13", "11", "10" }
            } );
            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }
    
    private void assertNodes( Traverser traverser, String... expectedNodes )
    {
        Set<String> set = new HashSet<String>( Arrays.asList( expectedNodes ) );
        for ( Node node : traverser )
        {
            assertTrue( set.remove( node.getProperty( "node.test.id" ) ) );
        }
        assertTrue( set.isEmpty() );
    }

    private void assertLevelsOfNodes( Traverser traverser, String[][] nodes )
    {
        Map<Integer, Collection<String>> map = new HashMap<Integer, Collection<String>>();
        for ( Node node : traverser )
        {
            Collection<String> collection = map.get( traverser.currentPosition().depth() );
            if ( collection == null )
            {
                collection = new ArrayList<String>();
                map.put( traverser.currentPosition().depth(), collection );
            }
            String name = (String) node.getProperty( "node.test.id" );
            collection.add( name );
        }
        
        for ( int i = 0; i < nodes.length; i++ )
        {
            Collection<String> expected = Arrays.asList( nodes[i] );
            assertEquals( expected, map.get( i ) );
        }
    }

    // Traverses the test "ise-tree-like" population breadth first,
    // but only traverses "ise" (TEST) relationships (the population also
    // contains
    // "ise_clone" (TEST_TRAVERSAL) rels)
    @Test
    public void testMultiRelBreadthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };
        Traverser traverser = root.traverse( BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH );

        try
        {
            this.assertLevelsOfNodes( traverser, new String[][] {
                new String[] { "1" },
                new String[] { "4", "2", "3" },
                new String[] { "5", "6", "7" },
                new String[] { "11", "10", "13", "12" },
            } );
            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Traverses the test "ise-tree-like" population breadth first,
    // starting in the middle of the tree and traversing only in the
    // "forward" direction
    @Test
    public void testDirectedBreadthTraversal() throws Exception
    {
        // Build test population
        Node root = this.buildIseTreePopulation();
        Node startNode = null;

        // Get a node in the middle of the tree:
        try
        {
            // a) Construct a returnable evaluator that returns node 2
            ReturnableEvaluator returnEvaluator = new ReturnableEvaluator()
            {
                @Override
                public boolean isReturnableNode( TraversalPosition pos )
                {
                    try
                    {
                        Node node = pos.currentNode();
                        String key = "node.test.id";
                        String nodeId = (String) node.getProperty( key );
                        return nodeId.equals( "2" );
                    }
                    catch ( Exception e )
                    {
                        return false;
                    }
                }
            };

            // b) create a traverser
            Traverser toTheMiddleTraverser = root.traverse( BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH, returnEvaluator, MyRelTypes.TEST,
                Direction.BOTH );

            // c) get the first node it returns
            startNode = toTheMiddleTraverser.iterator().next();
            assertEquals( "2", startNode.getProperty( "node.test.id" ) );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "Something went wrong when trying to get a start node "
                + "in the middle of the tree: " + e );
        }

        // Construct the real traverser
        Traverser traverser = startNode.traverse( BREADTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL,
            MyRelTypes.TEST, Direction.OUTGOING );
        try
        {
            assertNextNodeId( traverser, "2" );
            assertNextNodeId( traverser, "6" );
            assertNextNodeId( traverser, "5" );
            assertNextNodeId( traverser, "11" );
            assertNextNodeId( traverser, "10" );
            assertNextNodeId( traverser, "13" );
            assertNextNodeId( traverser, "12" );
            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            nsee.printStackTrace();
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Traverses the full test "ise-tree-like" population depth first
    // and verifies that it is returned in correct order
    @Test
    public void testBruteDepthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL };
        Traverser traverser = root.traverse( DEPTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH, traversableRels[1],
            Direction.BOTH );

        try
        {
            this.assertNodes( traverser, "1", "2", "3", "4", "5", "6", "7",
                    "8", "9", "10", "11", "12", "13", "14" );
            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Traverses the test "ise-tree-like" population depth first,
    // but only traverses "ise" relationships (the population also contains
    // "ise_clone" rels)
    @Test
    public void testMultiRelDepthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };
        Traverser traverser = root.traverse( DEPTH_FIRST,
            StopEvaluator.END_OF_GRAPH, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH );

        try
        {
            assertNodes( traverser, "1", "2", "3", "4", "5", "6", "7",
                    "10", "11", "12", "13" );
            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Verifies that the stop evaluator can stop based on the current node
    @Test
    public void testStopOnCurrentNode() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct a stop evaluator that stops on nodes 5, 6, 3 and 4
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            @Override
            public boolean isStopNode( TraversalPosition position )
            {
                try
                {
                    Node node = position.currentNode();

                    String nodeId = (String) node.getProperty( "node.test.id" );
                    return nodeId.equals( "5" ) || nodeId.equals( "6" )
                        || nodeId.equals( "3" ) || nodeId.equals( "4" );
                }
                catch ( Exception e )
                {
                    return false;
                }
            }
        };

        // Create a traverser
        Traverser traverser = root.traverse( BREADTH_FIRST, stopEvaluator,
            ReturnableEvaluator.ALL, traversableRels[0], Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "5" );

            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Verifies that the stop evaluator can stop based on the previous node
    @Test
    public void testStopOnPreviousNode() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct a stop evaluator that stops on nodes 2, 3, and 4
        // (ie root's children)
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            @Override
            public boolean isStopNode( TraversalPosition position )
            {
                try
                {
                    Node node = position.previousNode();
                    String nodeId = (String) node.getProperty( "node.test.id" );
                    return nodeId.equals( "1" );
                }
                catch ( Exception e )
                {
                    return false;
                }
            }
        };

        // Create a traverser
        Traverser traverser = root.traverse( BREADTH_FIRST, stopEvaluator,
            ReturnableEvaluator.ALL, traversableRels[0], Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );

            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            // Delete ise tree and commmit work
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Verifies that the stop evaluator can stop based on the current depth
    @Test
    public void testStopOnDepth() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct a stop evaluator that stops on depth 2
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            @Override
            public boolean isStopNode( TraversalPosition position )
            {
                return position.depth() >= 2;
            }
        };

        // Create a traverser
        Traverser traverser = root.traverse( BREADTH_FIRST, stopEvaluator,
            ReturnableEvaluator.ALL, traversableRels[0], Direction.BOTH );

        try
        {
            assertNextNodeId( traverser, "1" );
            assertNextNodeId( traverser, "2" );
            assertNextNodeId( traverser, "3" );
            assertNextNodeId( traverser, "4" );
            assertNextNodeId( traverser, "5" );
            assertNextNodeId( traverser, "6" );
            assertNextNodeId( traverser, "7" );

            assertTrue( "Too many nodes returned from traversal", !traverser.iterator().hasNext() );
        }
        finally
        {
            // Delete ise tree and commmit work
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Verifies that the stop evaluator can stop based on the amount of
    // returned nodes
    @Test
    public void testStopOnReturnedNodes() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct stop- and returnable evaluators that return 5 nodes
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            @Override
            public boolean isStopNode( TraversalPosition position )
            {
                // Stop traversing when we've returned 5 nodes
                return position.returnedNodesCount() >= 5;
            }
        };
        ReturnableEvaluator returnEvaluator = new ReturnableEvaluator()
        {
            @Override
            public boolean isReturnableNode( TraversalPosition position )
            {
                // Return nodes until we've reached 5 nodes or end of graph
                return position.returnedNodesCount() < 5;
            }
        };

        // Create a traverser
        Traverser traverser = root.traverse( BREADTH_FIRST, stopEvaluator,
            returnEvaluator, traversableRels[0], Direction.BOTH );

        try
        {
            this.assertLevelsOfNodes( traverser, new String[][] {
                new String[] { "1" },
                new String[] { "2", "4", "3" },
                new String[] { "5" },
            } );

            assertTrue( "Too many nodes returned from traversal", traverser
                .iterator().hasNext() == false );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            // Delete ise tree and commmit work
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Verifies that the stop evaluator can stop based on the last
    // traversed relationship
    @Test
    public void testStopOnLastRelationship() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL };

        // Construct stop- and returnable evaluators that return 5 nodes
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            @Override
            public boolean isStopNode( TraversalPosition position )
            {
                // Stop when we got here by traversing a clone relationship
                Relationship rel = position.lastRelationshipTraversed();
                return rel != null && rel.isType( MyRelTypes.TEST_TRAVERSAL );
            }
        };

        // Create a traverser
        Traverser traverser = root.traverse( BREADTH_FIRST, stopEvaluator,
            ReturnableEvaluator.ALL, traversableRels[0], Direction.BOTH,
            traversableRels[1], Direction.BOTH );

        try
        {
            this.assertLevelsOfNodes( traverser, new String[][] {
                new String[] { "1" },
                new String[] { "2", "3", "4" },
                new String[] { "5", "6", "7", "8", "9" },
                new String[] { "11", "10", "13", "12" }
            } );

            assertTrue( "Too many nodes returned from traversal", !traverser.iterator().hasNext() );
        }
        catch ( java.util.NoSuchElementException nsee )
        {
            fail( "Too few nodes returned from traversal" );
        }
        finally
        {
            // Delete ise tree and commmit work
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // -- Utility operations

    private Node buildIseTreePopulation() throws Exception
    {
        // Create population
        Node[] nodeSpace = new Node[] { null, // empty
            getGraphDb().createNode(), // 1 [root]
            getGraphDb().createNode(), // 2
            getGraphDb().createNode(), // 3
            getGraphDb().createNode(), // 4
            getGraphDb().createNode(), // 5
            getGraphDb().createNode(), // 6
            getGraphDb().createNode(), // 7
            getGraphDb().createNode(), // 8
            getGraphDb().createNode(), // 9
            getGraphDb().createNode(), // 10
            getGraphDb().createNode(), // 11
            getGraphDb().createNode(), // 12
            getGraphDb().createNode(), // 13
            getGraphDb().createNode(), // 14
        };

        String key = "node.test.id";
        for ( int i = 1; i < nodeSpace.length; i++ )
        {
            nodeSpace[i].setProperty( key, "" + i );
        }

        RelationshipType ise = MyRelTypes.TEST;
        RelationshipType clone = MyRelTypes.TEST_TRAVERSAL;

        // Bind it together
        // 
        //               ----(1)-------
        //              /      \       \
        //          --(2)--    (3)     (4)--
        //         /       \     \      |   \
        //     --(5)----- (6)---(7)   (8)  (9)
        //    /   |   \  \                  |
        //  (10) (11)(12)(13)              (14)
        //
        
        nodeSpace[1].createRelationshipTo( nodeSpace[2], ise );
        nodeSpace[2].createRelationshipTo( nodeSpace[5], ise );
        nodeSpace[5].createRelationshipTo( nodeSpace[10], ise );
        nodeSpace[5].createRelationshipTo( nodeSpace[11], ise );
        nodeSpace[5].createRelationshipTo( nodeSpace[12], ise );
        nodeSpace[5].createRelationshipTo( nodeSpace[13], ise );
        nodeSpace[2].createRelationshipTo( nodeSpace[6], ise );
        nodeSpace[1].createRelationshipTo( nodeSpace[3], ise );
        nodeSpace[1].createRelationshipTo( nodeSpace[4], ise );
        nodeSpace[3].createRelationshipTo( nodeSpace[7], ise );

        nodeSpace[6].createRelationshipTo( nodeSpace[7], clone );
        nodeSpace[4].createRelationshipTo( nodeSpace[8], clone );
        nodeSpace[4].createRelationshipTo( nodeSpace[9], clone );
        nodeSpace[9].createRelationshipTo( nodeSpace[14], clone );

        return nodeSpace[1]; // root
    }

    // Deletes a tree-like structure of nodes, starting with 'currentNode'.
    // Works fine with trees, dies horribly on cyclic structures.
    private void deleteNodeTreeRecursively( Node currentNode, int depth )
    {
        if ( depth > 100 )
        {
            throw new RuntimeException( "Recursive guard: depth = " + depth );
        }

        if ( currentNode == null )
        {
            return;
        }

        Iterable<Relationship> rels = currentNode.getRelationships();
        for ( Relationship rel : rels )
        {
            if ( !rel.getStartNode().equals( currentNode ) )
            {
                continue;
            }
            Node endNode = rel.getEndNode();
            rel.delete();
            this.deleteNodeTreeRecursively( endNode, depth + 1 );
        }
        String msg = "Deleting " + currentNode + "\t[";
        String id = (String) currentNode.getProperty( "node.test.id" );
        msg += id + "]";

        Iterable<Relationship> allRels = currentNode.getRelationships();
        for ( Relationship rel : allRels )
        {
            rel.delete();
        }
        currentNode.delete();
    }

    private void assertNextNodeId( Traverser traverser, String property )
        throws NotFoundException
    {
        Node node = traverser.iterator().next();
        assertEquals( property, node.getProperty( "node.test.id" ) );
    }
}
