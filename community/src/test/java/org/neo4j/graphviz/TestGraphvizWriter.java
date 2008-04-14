package org.neo4j.graphviz;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.LinkedList;

import junit.framework.Assert;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Traverser;

public class TestGraphvizWriter
{
	enum type implements RelationshipType
	{
		KNOWS, WORKS_FOR
	}

	private Mockery mockery;
	private Traverser traverser;

	@Before
	public void setUp()
	{
		mockery = new Mockery();
		traverser = mockery.mock( Traverser.class );
	}

	@After
	public void tearDown()
	{
		mockery.assertIsSatisfied();
	}

	@Test
	public void testSimpleGraph() throws Exception
	{
		final Node emil = mockery.mock( Node.class, "emil" );
		final Node tobias = mockery.mock( Node.class, "tobias" );
		final Node johan = mockery.mock( Node.class, "johan" );
		final Relationship emilKNOWStobias = mockery.mock( Relationship.class,
		    "emilKNOWStobias" );
		final Relationship johanKNOWSemil = mockery.mock( Relationship.class,
		    "johanKNOWSemil" );
		final Relationship tobiasKNOWSjohan = mockery.mock( Relationship.class,
		    "tobiasKNOWSjohan" );
		final Relationship tobiasWORKS_FORemil = mockery.mock(
		    Relationship.class, "tobiasWORKS_FORemil" );
		// FIXME: this is why we NEED NeoMock
		mockery.checking( new Expectations()
		{
			{
				one( traverser ).iterator();
				will( returnValue( ( new LinkedList<Node>()
				{
					{
						add( emil );
						add( tobias );
						add( johan );
					}
				} ).iterator() ) );
				// id's
				allowing( emil ).getId();
				will( returnValue( 1L ) );
				allowing( tobias ).getId();
				will( returnValue( 2L ) );
				allowing( johan ).getId();
				will( returnValue( 3L ) );
				allowing( emilKNOWStobias ).getId();
				will( returnValue( 1L ) );
				allowing( tobiasKNOWSjohan ).getId();
				will( returnValue( 2L ) );
				allowing( tobiasWORKS_FORemil ).getId();
				will( returnValue( 3L ) );
				allowing( johanKNOWSemil ).getId();
				will( returnValue( 4L ) );
				// properties
				one( emil ).getPropertyKeys();
				will( returnValue( new LinkedList<String>()
				{
					{
						add( "name" );
						add( "age" );
						add( "junk" );
					}
				} ) );
				one( emil ).getProperty( "name" );
				will( returnValue( "Emil Eifrem" ) );
				one( emil ).getProperty( "age" );
				will( returnValue( 29 ) );
				one( emil ).getProperty( "junk" );
				will( returnValue( "Should not end up in the picture" ) );
				one( tobias ).getPropertyKeys();
				will( returnValue( new LinkedList<String>()
				{
					{
						add( "name" );
						add( "age" );
					}
				} ) );
				one( tobias ).getProperty( "name" );
				will( returnValue( "Tobias \"snuttis\" Ivarsson" ) );
				one( tobias ).getProperty( "age" );
				will( returnValue( 23 ) );
				one( johan ).getPropertyKeys();
				will( returnValue( new LinkedList<String>()
				{
					{
						add( "name" );
					}
				} ) );
				one( johan ).getProperty( "name" );
				will( returnValue( "Johan '\\n00b' Svensson" ) );
				// Relationships
				one( emil ).getRelationships();
				will( returnValue( new LinkedList<Relationship>()
				{
					{
						add( emilKNOWStobias );
						add( tobiasWORKS_FORemil );
						add( johanKNOWSemil );
					}
				} ) );
				one( tobias ).getRelationships();
				will( returnValue( new LinkedList<Relationship>()
				{
					{
						add( emilKNOWStobias );
						add( tobiasWORKS_FORemil );
						add( tobiasKNOWSjohan );
					}
				} ) );
				one( johan ).getRelationships();
				will( returnValue( new LinkedList<Relationship>()
				{
					{
						add( johanKNOWSemil );
						add( tobiasKNOWSjohan );
					}
				} ) );
				// member nodes of properties
				allowing( emilKNOWStobias ).getStartNode();
				will( returnValue( emil ) );
				allowing( emilKNOWStobias ).getEndNode();
				will( returnValue( tobias ) );
				allowing( tobiasKNOWSjohan ).getStartNode();
				will( returnValue( tobias ) );
				allowing( tobiasKNOWSjohan ).getEndNode();
				will( returnValue( johan ) );
				allowing( tobiasWORKS_FORemil ).getStartNode();
				will( returnValue( tobias ) );
				allowing( tobiasWORKS_FORemil ).getEndNode();
				will( returnValue( emil ) );
				allowing( johanKNOWSemil ).getStartNode();
				will( returnValue( johan ) );
				allowing( johanKNOWSemil ).getEndNode();
				will( returnValue( emil ) );
				// relationship properties
				one( emilKNOWStobias ).getType();
				will( returnValue( type.KNOWS ) );
				one( johanKNOWSemil ).getType();
				will( returnValue( type.KNOWS ) );
				one( tobiasKNOWSjohan ).getType();
				will( returnValue( type.KNOWS ) );
				one( tobiasWORKS_FORemil ).getType();
				will( returnValue( type.WORKS_FOR ) );
				one( emilKNOWStobias ).getPropertyKeys();
				will( returnValue( new LinkedList<String>()
				{
					{
						add( "since" );
						add( "junk" );
					}
				} ) );
				one( emilKNOWStobias ).getProperty( "since" );
				will( returnValue( "2003-08-17" ) );
				one( emilKNOWStobias ).getProperty( "junk" );
				will( returnValue( "should not end up in the picture" ) );
				one( johanKNOWSemil ).getPropertyKeys();
				will( returnValue( new LinkedList<String>() ) );
				one( tobiasKNOWSjohan ).getPropertyKeys();
				will( returnValue( new LinkedList<String>() ) );
				one( tobiasWORKS_FORemil ).getPropertyKeys();
				will( returnValue( new LinkedList<String>() ) );
			}
		} );
		// perform the test
		OutputStream out = new ByteArrayOutputStream();
		EmissionPolicy policy = new EmissionPolicy()
		{
			public boolean acceptProperty( SourceType source, String key )
			{
				return !key.equals( "junk" );
			}
		};
		new GraphvizWriter( out, policy ).consume( traverser );
		// the correct output (split into parts since there is no guarantee of
		// order)
		String start = "digraph Neo {\n"
		    + "  fontname = \"Bitstream Vera Sans\"\n" + "  fontsize = 8\n"
		    + "  node [\n" + "    fontname = \"Bitstream Vera Sans\"\n"
		    + "    fontsize = 8\n" + "    shape = \"Mrecord\"\n" + "  ]\n"
		    + "  edge [\n" + "    fontname = \"Bitstream Vera Sans\"\n"
		    + "    fontsize = 8\n" + "  ]\n";
		String n1 = "  N1 [\n"
		    + "    label = \"{Node[1]|name = 'Emil Eifrem' : String\\lage = 29 : int}\"\n"
		    + "  ]\n";
		String n2 = "  N2 [\n"
		    + "    label = \"{Node[2]|name = 'Tobias \\\"snuttis\\\" Ivarsson' : String\\lage = 23 : int}\"\n"
		    + "  ]\n";
		String n3 = "  N3 [\n"
		    + "    label = \"{Node[3]|name = 'Johan \\\\'\\\\\\\\n00b\\\\' Svensson' : String}\"\n"
		    + "  ]\n";
		String n1n2 = "  N1 -> N2 [\n"
		    + "    label = \"KNOWS\\lsince = '2003-08-17' : String\"\n"
		    + "  ]\n";
		String n2n1 = "  N2 -> N1 [\n" + "    label = \"WORKS_FOR\"\n"
		    + "  ]\n";
		String n3n1 = "  N3 -> N1 [\n" + "    label = \"KNOWS\"\n" + "  ]\n";
		String n2n3 = "  N2 -> N3 [\n" + "    label = \"KNOWS\"\n" + "  ]\n";
		String end = "}\n";
		// verify the output
		String was = out.toString();
		System.out.println( "GraphvizWriter test, output was:" );
		System.out.print( was );
		Assert.assertEquals( "erronious output length", 666, was.length() );
		Assert.assertTrue( "erronious output start", was.startsWith( start ) );
		Assert.assertTrue( "erronious output end", was.endsWith( end ) );
		int i = 0;
		for ( String part : new String[] { n1, n2, n3, n1n2, n2n1, n2n3, n3n1 } )
		{
			Assert.assertTrue( "erronious part (" + ( i++ ) + ") of message",
			    was.contains( part ) );
		}
	}
}
