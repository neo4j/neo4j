/*
 * Copyright 2007 Network Engine for Objects in Lund AB [neotechnology.com]
 */
package org.neo4j.graphviz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;

import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;

/**
 * This class is used for writing a traverser to a stream as a graphviz
 * representation.
 * @author Tobias Ivarsson
 */
public class GraphvizWriter extends EmittingConsumer
{
	private final GraphvizEmitter emitter;

	/**
	 * Construct a new writer that writes to a given stream.
	 * @param stream
	 *            The stream to write to.
	 */
	public GraphvizWriter( OutputStream stream )
	{
		this( stream, System.getProperties() );
	}

	/**
	 * Construct a new writer that writes to a given stream.
	 * @param stream
	 *            The stream to write to.
	 * @param properties
	 *            The properties that contain the Graphviz formatting
	 *            information, and information on how to build an
	 *            {@link EmissionPolicy}.
	 */
	public GraphvizWriter( OutputStream stream, Properties properties )
	{
		this( stream, EmissionPolicyFactory
		    .buildPolicyFromProperties( properties ), properties );
	}

	/**
	 * Construct a new writer that writes to a given stream.
	 * @param stream
	 *            The stream to write to.
	 * @param policy
	 *            The policy that determines what to print.
	 */
	public GraphvizWriter( OutputStream stream, EmissionPolicy policy )
	{
		this( stream, policy, System.getProperties() );
	}

	/**
	 * Construct a new writer that writes to a given stream.
	 * @param stream
	 *            The stream to write to.
	 * @param policy
	 *            The policy that determines what to print.
	 * @param properties
	 *            The properties that contain the Graphviz formatting
	 *            information.
	 */
	public GraphvizWriter( OutputStream stream, EmissionPolicy policy,
	    Properties properties )
	{
		final PrintStream output = new PrintStream( stream );
		this.emitter = new GraphvizEmitter( policy, properties )
		{
			@Override
			void print( String data )
			{
				output.print( data );
			}

			@Override
			void println( String data )
			{
				output.println( data );
			}
		};
	}

	/**
	 * Writes a graphviz file from the given Traverser to the given file.
	 * @param file
	 *            The file to write the representation of the traverser to.
	 * @param nodes
	 *            The nodes that define the subgraph to write.
	 * @throws IOException
	 *             If opening the file or writing the contents of the file
	 *             fails.
	 */
	public static void write( File file, Iterable<Node> nodes )
	    throws IOException
	{
		FileOutputStream stream = new FileOutputStream( file );
		GraphvizWriter writer = new GraphvizWriter( stream );
		writer.consume( nodes );
		stream.close();
	}

	/**
	 * Writes a graphviz file from the given Traverser to the given file.
	 * @param file
	 *            The file to write the representation of the traverser to.
	 * @param nodes
	 *            The nodes that define the subgraph to write.
	 * @param policy
	 *            The policy that determines what to print.
	 * @throws IOException
	 *             If opening the file or writing the contents of the file
	 *             fails.
	 */
	public static void write( File file, Iterable<Node> nodes,
	    EmissionPolicy policy ) throws IOException
	{
		FileOutputStream stream = new FileOutputStream( file );
		GraphvizWriter writer = new GraphvizWriter( stream, policy );
		writer.consume( nodes );
		stream.close();
	}

	/**
	 * Writes a graphviz file from the given Traverser to the given file.
	 * @param file
	 *            The file to write the representation of the traverser to.
	 * @param nodes
	 *            The nodes that define the subgraph to write.
	 * @param policy
	 *            The policy that determines what to print.
	 * @param properties
	 *            The properties that contain the Graphviz formatting
	 *            information.
	 * @throws IOException
	 *             If opening the file or writing the contents of the file
	 *             fails.
	 */
	public static void write( File file, Iterable<Node> nodes,
	    EmissionPolicy policy, Properties properties ) throws IOException
	{
		FileOutputStream stream = new FileOutputStream( file );
		GraphvizWriter writer = new GraphvizWriter( stream, policy, properties );
		writer.consume( nodes );
		stream.close();
	}

	/**
	 * Writes a graphviz file from the given Traverser to the given file.
	 * @param file
	 *            The file to write the representation of the traverser to.
	 * @param nodes
	 *            The nodes that define the subgraph to write.
	 * @param properties
	 *            The properties that contain the Graphviz formatting
	 *            information, and information on how to build an
	 *            {@link EmissionPolicy}.
	 * @throws IOException
	 *             If opening the file or writing the contents of the file
	 *             fails.
	 */
	public static void write( File file, Iterable<Node> nodes,
	    Properties properties ) throws IOException
	{
		FileOutputStream stream = new FileOutputStream( file );
		GraphvizWriter writer = new GraphvizWriter( stream, properties );
		writer.consume( nodes );
		stream.close();
	}

	@Override
	public void consume( Iterable<Node> nodes )
	{
		emitter.println( "digraph Neo {" );
		emitter.emitHeader();
		super.consume( nodes );
		emitter.done();
	}

	@Override
	protected Emitter emitNode( long id )
	{
		emitter.print( "  N" + id + " [\n    label = \"{Node[" + id + "]|" );
		emitter.push( "}\"\n  ]" );
		emitter.softline = false;
		return emitter;
	}

	@Override
	protected Emitter emitRelationship( long id, RelationshipType type,
	    long start, long end )
	{
		emitter.print( "  N" + start + " -> N" + end + " [\n    label = \""
		    + type.name() );
		emitter.push( "\"\n  ]" );
		emitter.softline = true;
		return emitter;
	}

	private static abstract class GraphvizEmitter extends Emitter
	{
		private static final String GRAPHVIZ_PREFIX = "neo.emit.graphviz.";
		private static final String NODE_PREFIX = GRAPHVIZ_PREFIX + "node.";
		private static final String EDGE_PREFIX = GRAPHVIZ_PREFIX
		    + "relationship.";
		private final Stack<String> endTokens;
		private final Properties properties;

		GraphvizEmitter( EmissionPolicy policy, Properties prop )
		{
			super( policy );
			this.properties = prop;
			endTokens = new Stack<String>();
			endTokens.push( "}" );
		}

		void emitHeader()
		{
			emitConfigLine( 1, "fontname", GRAPHVIZ_PREFIX + "fontname",
			    "\"Bitstream Vera Sans\"" );
			emitConfigLine( 1, "fontsize", GRAPHVIZ_PREFIX + "fontsize", "8" );
			Set<String> nodeProp = new HashSet<String>();
			Set<String> edgeProp = new HashSet<String>();
			for ( String prop : properties.stringPropertyNames() )
			{
				if ( prop.startsWith( NODE_PREFIX ) )
				{
					nodeProp.add( prop.substring( NODE_PREFIX.length() ) );
				}
				else if ( prop.startsWith( EDGE_PREFIX ) )
				{
					edgeProp.add( prop.substring( EDGE_PREFIX.length() ) );
				}
			}
			// Node settings
			println( "  node [" );
			emitConfigLine( 2, "fontname", NODE_PREFIX + "fontname",
			    "\"Bitstream Vera Sans\"" );
			nodeProp.remove( "fontname" );
			emitConfigLine( 2, "fontsize", NODE_PREFIX + "fontsize", "8" );
			nodeProp.remove( "fontsize" );
			emitConfigLine( 2, "shape", NODE_PREFIX + "shape", "\"Mrecord\"" );
			nodeProp.remove( "shape" );
			for ( String prop : nodeProp )
			{
				emitConfigLine( 2, prop, NODE_PREFIX + prop, "" );
			}
			println( "  ]" );
			// Relationship setting
			println( "  edge [" );
			emitConfigLine( 2, "fontname", EDGE_PREFIX + "fontname",
			    "\"Bitstream Vera Sans\"" );
			edgeProp.remove( "fontname" );
			emitConfigLine( 2, "fontsize", EDGE_PREFIX + "fontsize", "8" );
			edgeProp.remove( "fontsize" );
			for ( String prop : edgeProp )
			{
				emitConfigLine( 2, prop, EDGE_PREFIX + prop, "" );
			}
			println( "  ]" );
		}

		private void emitConfigLine( int indentation, String propName,
		    String propKey, String defaultValue )
		{
			String propValue = properties.getProperty( propKey, defaultValue );
			for ( int i = 0; i < indentation; i++ )
			{
				print( "  " );
			}
			print( propName );
			print( " = " );
			println( propValue );
		}

		boolean softline = false;

		void push( String endToken )
		{
			endTokens.push( endToken );
		}

		@Override
		protected void emitMapping( String key, String value, String type )
		{
			if ( softline )
			{
				print( "\\l" );
			}
			print( key + " = " + value + " : " + type );
			softline = true;
		}

		@Override
		public void done()
		{
			softline = false;
			println( endTokens.pop() );
		}

		abstract void print( String data );

		abstract void println( String data );
	}
}
