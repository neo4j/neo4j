/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.traversal;

import static org.neo4j.api.core.Traverser.Order.BREADTH_FIRST;
import static org.neo4j.api.core.Traverser.Order.DEPTH_FIRST;
import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.AbstractNeoTestCase;
import org.neo4j.impl.MyRelTypes;

public class TestTraversal extends AbstractNeoTestCase
{
    public TestTraversal( String testName )
    {
        super( testName );
    }

    // -- Test operations

    // Tests the traverser factory for sanity checks with corrupted input
    public void testSanityChecks1() throws Exception
    {
        // Valid data
        Node root = getNeo().createNode();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST, };
        // Null traversable relationships
        this.sanityCheckTraverser( "Sanity check failed: null traversable "
            + "rels should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, null, Direction.OUTGOING,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );

        // Null stop evaluator
        this.sanityCheckTraverser( "Sanity check failed: null stop eval "
            + "should throw an IllegalArgumentException", BREADTH_FIRST, root,
            traversableRels[0], Direction.OUTGOING, null,
            ReturnableEvaluator.ALL );

        // Null returnable evaluator
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], Direction.OUTGOING,
            StopEvaluator.END_OF_NETWORK, null );
        root.delete();
    }

    public void testSanityChecks2() throws Exception
    {
        // ------------- with traverser direction -------------
        // Valid data
        Node root = getNeo().createNode();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST, };
        Direction[] traversableDirs = new Direction[] { Direction.OUTGOING };

        // Null traversable relationships
        this.sanityCheckTraverser( "Sanity check failed: null traversable "
            + "rels should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, null, traversableDirs[0],
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );

        // Null traversable directions
        this.sanityCheckTraverser( "Sanity check failed: null traversable "
            + "rels should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], null,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );

        // Null stop evaluator
        this.sanityCheckTraverser( "Sanity check failed: null stop eval "
            + "should throw an IllegalArgumentException", BREADTH_FIRST, root,
            traversableRels[0], traversableDirs[0], null,
            ReturnableEvaluator.ALL );

        // Null returnable evaluator
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], traversableDirs[0],
            StopEvaluator.END_OF_NETWORK, null );
        // traversable relationships length not equal to traversable directions
        // length
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, traversableRels[0], null,
            StopEvaluator.END_OF_NETWORK, null );
        this.sanityCheckTraverser( "Sanity check failed: null returnable "
            + "evaluator should throw an " + "IllegalArgumentException",
            BREADTH_FIRST, root, null, traversableDirs[0],
            StopEvaluator.END_OF_NETWORK, null );
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

    private void sanityCheckTraverser( String failMessage, Order type,
        Node startNode, RelationshipType traversableRel1, Direction direction1,
        RelationshipType traversableRel2, Direction direction2,
        StopEvaluator stopEval, ReturnableEvaluator retEval )
    {
        try
        {
            startNode.traverse( type, stopEval, retEval, traversableRel1,
                direction1, traversableRel2, direction2 );
            fail( failMessage );
        }
        catch ( IllegalArgumentException iae )
        {
            // This is ok
        }
    }

    // Traverses the full test "ise-tree-like" population breadth first
    // and verifies that it is returned in correct order
    public void testBruteBreadthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL };
        Traverser traverser = root.traverse( BREADTH_FIRST,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH, traversableRels[1],
            Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "7" );
            this.assertNextNodeId( traverser, "8" );
            this.assertNextNodeId( traverser, "9" );
            this.assertNextNodeId( traverser, "10" );
            this.assertNextNodeId( traverser, "11" );
            this.assertNextNodeId( traverser, "12" );
            this.assertNextNodeId( traverser, "13" );
            this.assertNextNodeId( traverser, "14" );
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
    // but only traverses "ise" (TEST) relationships (the population also
    // contains
    // "ise_clone" (TEST_TRAVERSAL) rels)
    public void testMultiRelBreadthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };
        Traverser traverser = root.traverse( BREADTH_FIRST,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "7" );
            this.assertNextNodeId( traverser, "10" );
            this.assertNextNodeId( traverser, "11" );
            this.assertNextNodeId( traverser, "12" );
            this.assertNextNodeId( traverser, "13" );
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
                StopEvaluator.END_OF_NETWORK, returnEvaluator, MyRelTypes.TEST,
                Direction.BOTH );

            // c) get the first node it returns
            startNode = toTheMiddleTraverser.iterator().next();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            fail( "Something went wrong when trying to get a start node "
                + "in the middle of the tree: " + e );
        }

        // Construct the real traverser
        Traverser traverser = startNode.traverse( BREADTH_FIRST,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
            MyRelTypes.TEST, Direction.OUTGOING );
        try
        {
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "10" );
            this.assertNextNodeId( traverser, "11" );
            this.assertNextNodeId( traverser, "12" );
            this.assertNextNodeId( traverser, "13" );
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
    public void testBruteDepthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] {
            MyRelTypes.TEST, MyRelTypes.TEST_TRAVERSAL };
        Traverser traverser = root.traverse( DEPTH_FIRST,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH, traversableRels[1],
            Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "9" );
            this.assertNextNodeId( traverser, "14" );
            this.assertNextNodeId( traverser, "8" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "7" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "13" );
            this.assertNextNodeId( traverser, "12" );
            this.assertNextNodeId( traverser, "11" );
            this.assertNextNodeId( traverser, "10" );

            // this is another possible order, depending on traversal
            // implementation/PS
            // this.assertNextNodeId( traverser, "2" );
            // this.assertNextNodeId( traverser, "5" );
            // this.assertNextNodeId( traverser, "10" );
            // this.assertNextNodeId( traverser, "11" );
            // this.assertNextNodeId( traverser, "12" );
            // this.assertNextNodeId( traverser, "13" );
            // this.assertNextNodeId( traverser, "6" );
            // this.assertNextNodeId( traverser, "7" );
            // this.assertNextNodeId( traverser, "3" );
            // this.assertNextNodeId( traverser, "4" );
            // this.assertNextNodeId( traverser, "8" );
            // this.assertNextNodeId( traverser, "9" );
            // this.assertNextNodeId( traverser, "14" );
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
    public void testMultiRelDepthTraversal() throws Exception
    {
        Node root = this.buildIseTreePopulation();
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };
        Traverser traverser = root.traverse( DEPTH_FIRST,
            StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL,
            traversableRels[0], Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "7" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "13" );
            this.assertNextNodeId( traverser, "12" );
            this.assertNextNodeId( traverser, "11" );
            this.assertNextNodeId( traverser, "10" );

            // this is another possible order, depends on traversal
            // implementation/PS
            // this.assertNextNodeId( traverser, "2" );
            // this.assertNextNodeId( traverser, "5" );
            // this.assertNextNodeId( traverser, "10" );
            // this.assertNextNodeId( traverser, "11" );
            // this.assertNextNodeId( traverser, "12" );
            // this.assertNextNodeId( traverser, "13" );
            // this.assertNextNodeId( traverser, "6" );
            // this.assertNextNodeId( traverser, "3" );
            // this.assertNextNodeId( traverser, "7" );
            // this.assertNextNodeId( traverser, "4" );
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
    public void testStopOnCurrentNode() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct a stop evaluator that stops on nodes 5, 6, 3 and 4
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
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
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "6" );

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
    public void testStopOnDepth() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct a stop evaluator that stops on depth 2
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
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
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "7" );

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

    // Verifies that the stop evaluator can stop based on the amount of
    // returned nodes
    public void testStopOnReturnedNodes() throws Exception
    {
        // Build ise tree
        Node root = this.buildIseTreePopulation();

        // Traverse only ISE relationships
        RelationshipType[] traversableRels = new RelationshipType[] { MyRelTypes.TEST };

        // Construct stop- and returnable evaluators that return 5 nodes
        StopEvaluator stopEvaluator = new StopEvaluator()
        {
            public boolean isStopNode( TraversalPosition position )
            {
                // Stop traversing when we've returned 5 nodes
                return position.returnedNodesCount() >= 5;
            }
        };
        ReturnableEvaluator returnEvaluator = new ReturnableEvaluator()
        {
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
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );
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
            // Delete ise tree and commmit work
            this.deleteNodeTreeRecursively( root, 0 );
        }
    }

    // Verifies that the stop evaluator can stop based on the last
    // traversed relationship
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
            public boolean isStopNode( TraversalPosition position )
            {
                // Stop when we got here by traversing a clone relationship
                Relationship rel = position.lastRelationshipTraversed();
                return rel != null
                    && rel.getType() == MyRelTypes.TEST_TRAVERSAL;
            }
        };

        // Create a traverser
        Traverser traverser = root.traverse( BREADTH_FIRST, stopEvaluator,
            ReturnableEvaluator.ALL, traversableRels[0], Direction.BOTH,
            traversableRels[1], Direction.BOTH );

        try
        {
            this.assertNextNodeId( traverser, "1" );
            this.assertNextNodeId( traverser, "2" );
            this.assertNextNodeId( traverser, "3" );
            this.assertNextNodeId( traverser, "4" );
            this.assertNextNodeId( traverser, "5" );
            this.assertNextNodeId( traverser, "6" );
            this.assertNextNodeId( traverser, "7" );
            this.assertNextNodeId( traverser, "8" );
            this.assertNextNodeId( traverser, "9" );
            this.assertNextNodeId( traverser, "10" );
            this.assertNextNodeId( traverser, "11" );
            this.assertNextNodeId( traverser, "12" );
            this.assertNextNodeId( traverser, "13" );

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

    // -- Utility operations

    private Node buildIseTreePopulation()
    // throws CreateException
    {
        try
        {
            // Create population
            Node[] nodeSpace = new Node[] { null, // empty
                getNeo().createNode(), // 1 [root]
                getNeo().createNode(), // 2
                getNeo().createNode(), // 3
                getNeo().createNode(), // 4
                getNeo().createNode(), // 5
                getNeo().createNode(), // 6
                getNeo().createNode(), // 7
                getNeo().createNode(), // 8
                getNeo().createNode(), // 9
                getNeo().createNode(), // 10
                getNeo().createNode(), // 11
                getNeo().createNode(), // 12
                getNeo().createNode(), // 13
                getNeo().createNode(), // 14
            };

            String key = "node.test.id";
            for ( int i = 1; i < nodeSpace.length; i++ )
            {
                nodeSpace[i].setProperty( key, "" + i );
            }

            RelationshipType ise = MyRelTypes.TEST;
            RelationshipType clone = MyRelTypes.TEST_TRAVERSAL;

            // Bind it together
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
        catch ( Exception e )
        {
            throw new RuntimeException( "Failed to create population", e );
        }
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
        try
        {
            String msg = "Deleting " + currentNode + "\t[";
            String id = (String) currentNode.getProperty( "node.test.id" );
            msg += id + "]";
        }
        catch ( Exception e )
        {
            System.err.println( "Err gen msg: " + e );
        }

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
