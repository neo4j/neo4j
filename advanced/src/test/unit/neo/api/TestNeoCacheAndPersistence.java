/*
 * Copyright 2002-2007 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package unit.neo.api;

import java.util.ArrayList;
import java.util.Iterator;

import javax.transaction.Status;
import javax.transaction.UserTransaction;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.impl.core.IllegalValueException;
import org.neo4j.impl.core.NodeManager;
import org.neo4j.impl.core.NotFoundException;
import org.neo4j.impl.transaction.TransactionFactory;

import unit.neo.MyRelTypes;


public class TestNeoCacheAndPersistence extends TestCase
{
	public TestNeoCacheAndPersistence( String testName )
	{
		super( testName );
	}
	
	public static void main(java.lang.String[] args)
	{
		junit.textui.TestRunner.run( suite() );
	}
	
	public static Test suite()
	{
		TestSuite suite = new TestSuite( TestNeoCacheAndPersistence.class );
		return suite;
	}
	
	private int node1Id				= -1;
	private int node2Id				= -1;
	private String key1				= "key1";
	private String key2				= "key2";
	private Integer int1			= new Integer( 1 );
	private Integer int2			= new Integer( 2 );
	private String string1			= new String( "1" );
	private String string2			= new String( "2" );
	
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
			// create a persistent test population
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel = NodeManager.getManager().createRelationship( 
				node1, node2, MyRelTypes.TEST );
			node1Id = (int) node1.getId();
			node2Id = (int) node2.getId();
			node1.setProperty( key1, int1 );
			node1.setProperty( key2, string1 );
			node2.setProperty( key1, int2 );
			node2.setProperty( key2, string2 );
			rel.setProperty( key1, int1 );
			rel.setProperty( key2, string1 );
			ut.commit();
			NodeManager.getManager().clearCache();
			// start transaction for tests
			ut.begin();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "Failed to setup test population, " + e );
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
			ut.begin();
			Node node1 = NodeManager.getManager().getNodeById( node1Id );
			Node node2 = NodeManager.getManager().getNodeById( node2Id );
			node1.getSingleRelationship( MyRelTypes.TEST, 
				Direction.BOTH ).delete();
			node1.delete();
			node2.delete();
			ut.commit();
		}
		catch ( Exception e )
		{
			e.printStackTrace();
			fail( "Failed to end transaciton and cleanup test population, " + 
				e );
		}
	}

	public void testAddProperty()
	{
		try
		{
			String key3 = "key3";

			Node node1 = NodeManager.getManager().getNodeById( node1Id );
			Node node2 = NodeManager.getManager().getNodeById( node2Id );
			Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, 
				Direction.BOTH );
			// add new property
			node2.setProperty( key3, int1 );
			rel.setProperty( key3, int2 );
			assertTrue( node1.hasProperty( key1 ) );
			assertTrue( node2.hasProperty( key1 ) );
			assertTrue( node1.hasProperty( key2 ) );
			assertTrue( node2.hasProperty( key2 ) );
			assertTrue( !node1.hasProperty( key3 ) );
			assertTrue( node2.hasProperty( key3 ) );
			assertEquals( int1, node1.getProperty( key1 ) );
			assertEquals( int2, node2.getProperty( key1 ) );
			assertEquals( string1, node1.getProperty( key2 ) );
			assertEquals( string2, node2.getProperty( key2 ) );
			assertEquals( int1, rel.getProperty( key1 ) );
			assertEquals( string1, rel.getProperty( key2 ) );
			assertEquals( int2, rel.getProperty( key3 ) );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
	}
	
	public void testNodeRemoveProperty()
	{
		try
		{
			Node node1 = NodeManager.getManager().getNodeById( node1Id );
			Node node2 = NodeManager.getManager().getNodeById( node2Id );
			Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST,
				Direction.BOTH );

			// test remove property
			 assertEquals( 1, node1.removeProperty( key1 ) );
			 assertEquals( 2, node2.removeProperty( key1 ) );
			 assertEquals( 1, rel.removeProperty( key1 ) );
		}
		catch ( NotFoundException e )
		{
			e.printStackTrace();
			fail( "" + e );
		}
	}
	
	public void testNodeChangeProperty()
	{
		try
		{

			Node node1 = NodeManager.getManager().getNodeById( node1Id );
			Node node2 = NodeManager.getManager().getNodeById( node2Id );
			Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, 
				Direction.BOTH );
			
			// test change property
			node1.setProperty( key1, int2 );
			node2.setProperty( key1, int1 );
			rel.setProperty( key1, int2 );
		}
		catch ( IllegalValueException e )
		{
			fail( "" + e );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
	}
	
	public void testNodeGetProperties()
	{
		try
		{
			Node node1 = NodeManager.getManager().getNodeById( node1Id );

			assertTrue( !node1.hasProperty( null ) );
			Iterator<Object> values = node1.getPropertyValues().iterator();
			values.next(); values.next();
			Iterator<String> keys = node1.getPropertyKeys().iterator();
			keys.next(); keys.next();
			assertTrue( node1.hasProperty( key1 ) );
			assertTrue( node1.hasProperty( key2 ) );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
	} 

	private Relationship[] getRelationshipArray( 
		Iterable<Relationship> relsIterable )
	{
		ArrayList<Relationship> relList = new ArrayList<Relationship>();
		for ( Relationship rel : relsIterable )
		{
			relList.add( rel );
		}
		return relList.toArray( new Relationship[ relList.size() ] );
	}
	
	public void testDirectedRelationship1()
	{
		try
		{
			Node node1 = NodeManager.getManager().getNodeById( node1Id );
			Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, 
				Direction.BOTH );
			Node nodes[] = rel.getNodes();
			assertEquals( 2, nodes.length );

			Node node2 = NodeManager.getManager().getNodeById( node2Id );
			assertTrue( nodes[0].equals( node1 )  && nodes[1].equals( node2 ) ); 
			assertEquals( node1, rel.getStartNode() );
			assertEquals( node2, rel.getEndNode() );

			Relationship relArray[] =  getRelationshipArray( 
				node1.getRelationships( MyRelTypes.TEST, 
					Direction.OUTGOING ) );
			assertEquals( 1, relArray.length );
			assertEquals( rel, relArray[0] );
			relArray = getRelationshipArray( node2.getRelationships( 
				MyRelTypes.TEST, Direction.INCOMING ) );
			assertEquals( 1, relArray.length );
			assertEquals( rel, relArray[0] );
		}
		catch ( NotFoundException e )
		{
			fail( "" + e );
		}
	}
	
	public void testRelCountInSameTx()
	{
		try
		{
			Node node1 = NodeManager.getManager().createNode();
			Node node2 = NodeManager.getManager().createNode();
			Relationship rel = NodeManager.getManager().createRelationship( 
				node1, node2, MyRelTypes.TEST );
			assertEquals( 1, getRelationshipArray( 
				node1.getRelationships() ).length );
			assertEquals( 1, getRelationshipArray( 
				node2.getRelationships() ).length );
			rel.delete();
			assertEquals( 0, getRelationshipArray( 
				node1.getRelationships() ).length );
			assertEquals( 0, getRelationshipArray( 
				node2.getRelationships() ).length );
			node1.delete();
			node2.delete();
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}
	
	public void testGetDirectedRelationship()
	{
		try
		{
			Node node1 = NodeManager.getManager().getNodeById( node1Id );
			Relationship rel = node1.getSingleRelationship( MyRelTypes.TEST, 
				Direction.OUTGOING );
			assertEquals( int1, rel.getProperty( key1 ) );
		}
		catch ( Exception e )
		{
			fail( "" + e );
		}
	}
}
