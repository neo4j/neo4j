package unit.neo.api;

import static org.neo4j.api.core.Traverser.Order.BREADTH_FIRST;
import static org.neo4j.api.core.Traverser.Order.DEPTH_FIRST;

import java.util.Iterator;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.TraversalPosition;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.core.CreateException;
import org.neo4j.impl.core.DeleteException;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.transaction.TransactionFactory;
import org.neo4j.impl.traversal.TraverserFactory;

import unit.neo.MyRelTypes;

public class TestTraversal extends TestCase
{
	public TestTraversal( String testName )
	{
		super( testName );
	}
	
	public static void main( java.lang.String[] args )
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestTraversal.class );
		return suite;
	}
	
	public void setUp()
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		try
		{
			if ( ut.getStatus() != Status.STATUS_NO_TRANSACTION )
			{
				fail ( "Status is not STATUS_NO_TRANSACTION but: " + 
					ut.getStatus() );
			}
			ut.begin();
		}
		catch ( Exception e )
		{
			fail( "Failed to start transaction, " + e );
		}
	}

	public void tearDown()
	{
		UserTransaction ut = TransactionFactory.getUserTransaction();
		try
		{
			if ( ut.getStatus() == Status.STATUS_ACTIVE )
			{
				ut.commit();
			}
			else if ( ut.getStatus() == Status.STATUS_MARKED_ROLLBACK )
			{
				ut.rollback();
			}
			else if ( ut.getStatus() == Status.STATUS_NO_TRANSACTION )
			{
				// do nothing
			}
			else
			{
				System.out.println( "ARGH." );
				fail( "Unkown transaction status[" + ut.getStatus() + "]." );
			}
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "Failed to end transaciton, " + e );
		}
	}

	// -- Test operations
	
	// Tests the traverser factory for sanity checks with corrupted input
	public void testSanityChecks1()
		throws Exception
	{
		// Valid data
		Node root = NodeManager.getManager().createNode();
		RelationshipType[] traversableRels = new RelationshipType[]
													{
														MyRelTypes.TEST,
													};
		// Null start node
		this.sanityCheckTraverser
								(
									"Sanity check failed: null start node " +
									"should throw an IllegalArgumentException",
									BREADTH_FIRST,
									null,
									traversableRels,
									StopEvaluator.END_OF_NETWORK,
									ReturnableEvaluator.ALL
								);
		
		// Null traversable relationships
		this.sanityCheckTraverser
								(
									"Sanity check failed: null traversable " +
									"rels should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									null,
									StopEvaluator.END_OF_NETWORK,
									ReturnableEvaluator.ALL
								);
		
		// Null stop evaluator
		this.sanityCheckTraverser
								(
									"Sanity check failed: null stop eval " +
									"should throw an IllegalArgumentException",
									BREADTH_FIRST,
									root,
									traversableRels,
									null,
									ReturnableEvaluator.ALL
								);
		
		// Null returnable evaluator
		this.sanityCheckTraverser
								(
									"Sanity check failed: null returnable " +
									"evaluator should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									traversableRels,
									StopEvaluator.END_OF_NETWORK,
									null
								);
		root.delete();
	}

	public void testSanityChecks2()
		throws Exception
	{	
		// ------------- with traverser direction -------------
		// Valid data
		Node root = NodeManager.getManager().createNode();
		RelationshipType[] traversableRels = new RelationshipType[]
													{
														MyRelTypes.TEST,
													};
		Direction[] traversableDirs = new Direction[]
		{
			Direction.OUTGOING
		};
		
		// Null start node
		this.sanityCheckTraverser
								(
									"Sanity check failed: null start node " +
									"should throw an IllegalArgumentException",
									BREADTH_FIRST,
									null,
									traversableRels,
									traversableDirs, 
									StopEvaluator.END_OF_NETWORK,
									ReturnableEvaluator.ALL
								);
		
		// Null traversable relationships
		this.sanityCheckTraverser
								(
									"Sanity check failed: null traversable " +
									"rels should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									null,
									traversableDirs, 
									StopEvaluator.END_OF_NETWORK,
									ReturnableEvaluator.ALL
								);
		
		// Null traversable directions
		this.sanityCheckTraverser
								(
									"Sanity check failed: null traversable " +
									"rels should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									traversableRels, 
									null,
									StopEvaluator.END_OF_NETWORK,
									ReturnableEvaluator.ALL
								);

		// Null stop evaluator
		this.sanityCheckTraverser
								(
									"Sanity check failed: null stop eval " +
									"should throw an IllegalArgumentException",
									BREADTH_FIRST,
									root,
									traversableRels,
									traversableDirs, 
									null,
									ReturnableEvaluator.ALL
								);
		
		// Null returnable evaluator
		this.sanityCheckTraverser
								(
									"Sanity check failed: null returnable " +
									"evaluator should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									traversableRels,
									traversableDirs, 
									StopEvaluator.END_OF_NETWORK,
									null
								);
		// traversable relationships length not equal to traversable directions 
		// length
		this.sanityCheckTraverser
								(
									"Sanity check failed: null returnable " +
									"evaluator should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									traversableRels,
									new Direction[] {}, 
									StopEvaluator.END_OF_NETWORK,
									null
								);
		this.sanityCheckTraverser
								(
									"Sanity check failed: null returnable " +
									"evaluator should throw an " +
									"IllegalArgumentException",
									BREADTH_FIRST,
									root,
									new RelationshipType[] {},
									traversableDirs,
									StopEvaluator.END_OF_NETWORK,
									null
								);
		root.delete();
	}
	
	// Tests the traverser factory for simple corrupted (null) input, used
	// by testSanityChecks()
	private void sanityCheckTraverser
									(
										String failMessage,
										Order type,
										Node startNode,
										RelationshipType[] traversableRels,
										StopEvaluator stopEval,
										ReturnableEvaluator retEval
									)
	{
		try
		{
			TraverserFactory.getFactory().createTraverser( type, startNode,
														   traversableRels,
														   stopEval, retEval );
			fail( failMessage );
		}
		catch ( IllegalArgumentException iae )
		{
			// This is ok
		}
	}
	
	private void sanityCheckTraverser
									(
										String failMessage,
										Order type,
										Node startNode,
										RelationshipType[] traversableRels,
										Direction[] traversableDirs,
										StopEvaluator stopEval,
										ReturnableEvaluator retEval
									)
	{
		try
		{
			TraverserFactory.getFactory().createTraverser( type, startNode,
														   traversableRels,
														   traversableDirs,
														   stopEval, retEval );
			fail( failMessage );
		}
		catch ( IllegalArgumentException iae )
		{
			// This is ok
		}
	}
	// Traverses the full test "ise-tree-like" population breadth first
	// and verifies that it is returned in correct order
	public void testBruteBreadthTraversal()
		throws Exception
	{
		Node root = this.buildIseTreePopulation();
		RelationshipType[] traversableRels = new RelationshipType[] {
												MyRelTypes.TEST,
												MyRelTypes.TEST_TRAVERSAL
																	};
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												StopEvaluator.END_OF_NETWORK,
												ReturnableEvaluator.ALL
											);
		
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
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	// but only traverses "ise" (TEST) relationships (the population also contains
	// "ise_clone" (TEST_TRAVERSAL) rels)
	public void testMultiRelBreadthTraversal()
		throws Exception
	{
		Node root = this.buildIseTreePopulation();
		RelationshipType[] traversableRels = new RelationshipType[] {
														MyRelTypes.TEST
																	};
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												StopEvaluator.END_OF_NETWORK,
												ReturnableEvaluator.ALL
											);
		
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
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testDirectedBreadthTraversal()
		throws Exception
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
								String nodeId = (String) node.getProperty( 
									key );
								return nodeId.equals( "2" );
							}
							catch ( Exception e )
							{
								return false;
							}
						}
					};

			// b) create a traverser
			TraverserFactory factory = TraverserFactory.getFactory();
			Traverser toTheMiddleTraverser = factory.createTraverser
												(
													BREADTH_FIRST,
													root,
													MyRelTypes.TEST,
													Direction.BOTH,
													StopEvaluator.END_OF_NETWORK,
													returnEvaluator
												);

			// c) get the first node it returns
			startNode = toTheMiddleTraverser.iterator().next();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "Something went wrong when trying to get a start node " +
				  "in the middle of the tree: " + e );
		}
		
		// Construct the real traverser
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												startNode,
												MyRelTypes.TEST,
												Direction.OUTGOING,
												StopEvaluator.END_OF_NETWORK,
												ReturnableEvaluator.ALL
											);
		try
		{
			this.assertNextNodeId( traverser, "2" );
			this.assertNextNodeId( traverser, "5" );
			this.assertNextNodeId( traverser, "6" );
			this.assertNextNodeId( traverser, "10" );
			this.assertNextNodeId( traverser, "11" );
			this.assertNextNodeId( traverser, "12" );
			this.assertNextNodeId( traverser, "13" );
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testBruteDepthTraversal()
		throws Exception
	{
		Node root = this.buildIseTreePopulation();
		RelationshipType[] traversableRels = new RelationshipType[] {
												MyRelTypes.TEST,
												MyRelTypes.TEST_TRAVERSAL
																	};
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												DEPTH_FIRST,
												root,
												traversableRels,
												StopEvaluator.END_OF_NETWORK,
												ReturnableEvaluator.ALL
											);
		
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

// this is another possible order, depending on traversal implementation/PS			
//			this.assertNextNodeId( traverser, "2" );
//			this.assertNextNodeId( traverser, "5" );
//			this.assertNextNodeId( traverser, "10" );
//			this.assertNextNodeId( traverser, "11" );
//			this.assertNextNodeId( traverser, "12" );
//			this.assertNextNodeId( traverser, "13" );
//			this.assertNextNodeId( traverser, "6" );
//			this.assertNextNodeId( traverser, "7" );
//			this.assertNextNodeId( traverser, "3" );
//			this.assertNextNodeId( traverser, "4" );
//			this.assertNextNodeId( traverser, "8" );
//			this.assertNextNodeId( traverser, "9" );
//			this.assertNextNodeId( traverser, "14" );
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false );
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
	public void testMultiRelDepthTraversal()
		throws Exception
	{
		Node root = this.buildIseTreePopulation();
		RelationshipType[] traversableRels = new RelationshipType[] {
														MyRelTypes.TEST
																	};
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												DEPTH_FIRST,
												root,
												traversableRels,
												StopEvaluator.END_OF_NETWORK,
												ReturnableEvaluator.ALL
											);
		
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

// this is another possible order, depends on traversal implementation/PS
//			this.assertNextNodeId( traverser, "2" );
//			this.assertNextNodeId( traverser, "5" );
//			this.assertNextNodeId( traverser, "10" );
//			this.assertNextNodeId( traverser, "11" );
//			this.assertNextNodeId( traverser, "12" );
//			this.assertNextNodeId( traverser, "13" );
//			this.assertNextNodeId( traverser, "6" );
//			this.assertNextNodeId( traverser, "3" );
//			this.assertNextNodeId( traverser, "7" );
//			this.assertNextNodeId( traverser, "4" );
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testStopOnCurrentNode()
		throws Exception
	{
		// Build ise tree
		Node root = this.buildIseTreePopulation();
		
		// Traverse only ISE relationships
		RelationshipType[] traversableRels = new RelationshipType[] {
														MyRelTypes.TEST
																	};
																	
		// Construct a stop evaluator that stops on nodes 5, 6, 3 and 4
		StopEvaluator stopEvaluator = new StopEvaluator()
			{
				public boolean isStopNode( TraversalPosition position )
				{
					try
					{
						Node node = position.currentNode();
						
						String nodeId = (String) node.getProperty(
										"node.test.id" );
						return	nodeId.equals( "5" ) ||
								nodeId.equals( "6" ) ||
								nodeId.equals( "3" ) ||
								nodeId.equals( "4" );
					}
					catch ( Exception e )
					{
						return false;
					}
				}
			};
		
		// Create a traverser
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												stopEvaluator,
												ReturnableEvaluator.ALL
											);
		
		try
		{
			this.assertNextNodeId( traverser, "1" );
			this.assertNextNodeId( traverser, "2" );
			this.assertNextNodeId( traverser, "3" );
			this.assertNextNodeId( traverser, "4" );
			this.assertNextNodeId( traverser, "5" );
			this.assertNextNodeId( traverser, "6" );
			
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testStopOnPreviousNode()
		throws Exception
	{
		// Build ise tree
		Node root = this.buildIseTreePopulation();
		
		// Traverse only ISE relationships
		RelationshipType[] traversableRels = new RelationshipType[] {
														MyRelTypes.TEST
																	};
																	
		// Construct a stop evaluator that stops on nodes 2, 3, and 4
		// (ie root's children)
		StopEvaluator stopEvaluator = new StopEvaluator()
			{
				public boolean isStopNode( TraversalPosition position )
				{
					try
					{
						Node node = position.previousNode();
						String nodeId = (String) node.getProperty(
										"node.test.id" );
						return	nodeId.equals( "1" );
					}
					catch ( Exception e )
					{
						return false;
					}
				}
			};
		
		// Create a traverser
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												stopEvaluator,
												ReturnableEvaluator.ALL
											);
		
		try
		{
			this.assertNextNodeId( traverser, "1" );
			this.assertNextNodeId( traverser, "2" );
			this.assertNextNodeId( traverser, "3" );
			this.assertNextNodeId( traverser, "4" );
			
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testStopOnDepth()
		throws Exception
	{
		// Build ise tree
		Node root = this.buildIseTreePopulation();
		
		// Traverse only ISE relationships
		RelationshipType[] traversableRels = new RelationshipType[] {
														MyRelTypes.TEST
																	};
																	
		// Construct a stop evaluator that stops on depth 2
		StopEvaluator stopEvaluator = new StopEvaluator()
			{
				public boolean isStopNode( TraversalPosition position )
				{
					return position.depth() >= 2;
				}
			};
		
		// Create a traverser
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												stopEvaluator,
												ReturnableEvaluator.ALL
											);
		
		try
		{
			this.assertNextNodeId( traverser, "1" );
			this.assertNextNodeId( traverser, "2" );
			this.assertNextNodeId( traverser, "3" );
			this.assertNextNodeId( traverser, "4" );
			this.assertNextNodeId( traverser, "5" );
			this.assertNextNodeId( traverser, "6" );
			this.assertNextNodeId( traverser, "7" );
			
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testStopOnReturnedNodes()
		throws Exception
	{
		// Build ise tree
		Node root = this.buildIseTreePopulation();
		
		// Traverse only ISE relationships
		RelationshipType[] traversableRels = new RelationshipType[] {
														MyRelTypes.TEST
																	};
																	
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
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												stopEvaluator,
												returnEvaluator
											);
		
		try
		{
			this.assertNextNodeId( traverser, "1" );
			this.assertNextNodeId( traverser, "2" );
			this.assertNextNodeId( traverser, "3" );
			this.assertNextNodeId( traverser, "4" );
			this.assertNextNodeId( traverser, "5" );
			
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
	public void testStopOnLastRelationship()
		throws Exception
	{
		// Build ise tree
		Node root = this.buildIseTreePopulation();
		
		// Traverse only ISE relationships
		RelationshipType[] traversableRels = new RelationshipType[] {
												MyRelTypes.TEST,
												MyRelTypes.TEST_TRAVERSAL
																	};
																	
		// Construct stop- and returnable evaluators that return 5 nodes
		StopEvaluator stopEvaluator = new StopEvaluator()
			{
				public boolean isStopNode( TraversalPosition position )
				{
					// Stop when we got here by traversing a clone relationship
					Relationship rel = position.lastRelationshipTraversed();
					return	rel != null &&
							rel.getType() == MyRelTypes.TEST_TRAVERSAL;
				}
			};
		
		// Create a traverser
		Traverser traverser = TraverserFactory.getFactory().createTraverser
											(
												BREADTH_FIRST,
												root,
												traversableRels,
												stopEvaluator,
												ReturnableEvaluator.ALL
											);
		
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
			
			assertTrue( "Too many nodes returned from traversal",
						traverser.iterator().hasNext() == false);
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
//		throws CreateException
	{
		NodeManager mgr		= NodeManager.getManager();
		
		try
		{
			// Create population
			Node[] nodeSpace = new Node[]	{
												null,				// empty
												mgr.createNode(),	// 1 [root]
												mgr.createNode(),	// 2
												mgr.createNode(),	// 3
												mgr.createNode(),	// 4
												mgr.createNode(),	// 5
												mgr.createNode(),	// 6
												mgr.createNode(),	// 7
												mgr.createNode(),	// 8
												mgr.createNode(),	// 9
												mgr.createNode(),	// 10
												mgr.createNode(),	// 11
												mgr.createNode(),	// 12
												mgr.createNode(),	// 13
												mgr.createNode(),	// 14
											};

			String key = "node.test.id";
			for ( int i = 1; i < nodeSpace.length; i++ )
			{
				nodeSpace[i].setProperty( key, "" + i );
			}
			
			RelationshipType ise	= MyRelTypes.TEST;
			RelationshipType clone	= MyRelTypes.TEST_TRAVERSAL;
			
			// Bind it together
			mgr.createRelationship( nodeSpace[1], nodeSpace[2], ise );
			mgr.createRelationship( nodeSpace[2], nodeSpace[5], ise );
			mgr.createRelationship( nodeSpace[5], nodeSpace[10], ise );
			mgr.createRelationship( nodeSpace[5], nodeSpace[11], ise );
			mgr.createRelationship( nodeSpace[5], nodeSpace[12], ise );
			mgr.createRelationship( nodeSpace[5], nodeSpace[13], ise );
			mgr.createRelationship( nodeSpace[2], nodeSpace[6], ise );
			mgr.createRelationship( nodeSpace[1], nodeSpace[3], ise );
			mgr.createRelationship( nodeSpace[1], nodeSpace[4], ise );
			mgr.createRelationship( nodeSpace[3], nodeSpace[7], ise );
			
			mgr.createRelationship( nodeSpace[6], nodeSpace[7], clone );
			mgr.createRelationship( nodeSpace[4], nodeSpace[8], clone );
			mgr.createRelationship( nodeSpace[4], nodeSpace[9], clone );
			mgr.createRelationship( nodeSpace[9], nodeSpace[14], clone );
			
			return nodeSpace[1]; // root
		}
		catch ( Exception e )
		{
			throw new CreateException( "Failed to create population", e );
		}
	}
	
	// Deletes a tree-like structure of nodes, starting with 'currentNode'.
	// Works fine with trees, dies horribly on cyclic structures.
	private void deleteNodeTreeRecursively( Node currentNode, int depth )
		throws DeleteException
	{
		if ( depth > 100 )
		{
			throw new DeleteException( "Recursive guard: depth = " + depth );
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
			try
			{
				rel.delete();
			}
			catch ( DeleteException de )
			{
				System.err.println( "Unable to delete rel: " + rel );
			}
			this.deleteNodeTreeRecursively( endNode, depth + 1 );
		}
		try
		{
			try
			{
				String msg = "Deleting " + currentNode + "\t[";
				String id = (String) currentNode.getProperty(
										"node.test.id" );
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
		catch ( DeleteException de )
		{
			System.err.println( "Unable to delete node " + currentNode + ": " + de );
		}
	}

	private void assertNextNodeId( Traverser traverser, String property )
		throws NotFoundException
	{
		Node node = traverser.iterator().next();
		assertEquals( property,
					  node.getProperty( "node.test.id" ) );
	}
	
	public void testTraverseDeletedRelationship() 
	{
		NodeManager nm = NodeManager.getManager();
		Node startNode = nm.createNode();
		Node endNode1 = nm.createNode();
		Node endNode2 = nm.createNode();
		Relationship rel1 = nm.createRelationship( startNode, endNode1, 
			MyRelTypes.TEST );
		Relationship rel2 = nm.createRelationship( startNode, endNode2, 
			MyRelTypes.TEST );
		Traverser trav = TraverserFactory.getFactory().createTraverser( 
			BREADTH_FIRST, startNode, 
			new RelationshipType[] { MyRelTypes.TEST },
			StopEvaluator.END_OF_NETWORK, ReturnableEvaluator.ALL );
		Iterator<Node> itr = trav.iterator();
		assertEquals( startNode, itr.next() );
		Node node = itr.next();
		if ( node.equals( endNode1 ) )
		{
			rel2.delete();
			assertTrue( !itr.hasNext() );
			rel1.delete();
		}
		else if ( node.equals( endNode2 ) )
		{
			rel1.delete();
			assertTrue( !itr.hasNext() );
			rel2.delete();
		}
		else
		{
			fail( "unkown node" );
		}
		startNode.delete();
		endNode1.delete();
		endNode2.delete();
	}
}
