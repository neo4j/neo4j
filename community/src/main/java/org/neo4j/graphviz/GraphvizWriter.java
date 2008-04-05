/*
 * Copyright 2007 Network Engine for Objects in Lund AB [neotechnology.com]
 */
package org.neo4j.graphviz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Stack;

import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Traverser;

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
		final PrintStream output = new PrintStream( stream );
		this.emitter = new GraphvizEmitter()
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
	 * @param traverser
	 *            The traverser to get the graph from to write.
	 * @throws IOException
	 *             If opening the file or writing the contents of the file
	 *             fails.
	 */
	public static void write( File file, Traverser traverser )
	    throws IOException
	{
		FileOutputStream stream = new FileOutputStream( file );
		GraphvizWriter writer = new GraphvizWriter( stream );
		writer.consume( traverser );
		stream.close();
	}

	@Override
	public void consume( Traverser traverser )
	{
		emitter.println( "digraph Neo {" );
		emitter.println( "  fontname = \"Bitstream Vera Sans\"" );
		emitter.println( "  fontsize = 8" );
		emitter.println( "  node [" );
		emitter.println( "    fontname = \"Bitstream Vera Sans\"" );
		emitter.println( "    fontsize = 8" );
		emitter.println( "    shape = \"Mrecord\"" );
		emitter.println( "  ]" );
		emitter.println( "  edge [" );
		emitter.println( "    fontname = \"Bitstream Vera Sans\"" );
		emitter.println( "    fontsize = 8" );
		emitter.println( "  ]" );
		super.consume( traverser );
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
		private final Stack<String> endTokens;

		GraphvizEmitter()
		{
			endTokens = new Stack<String>();
			endTokens.push( "}" );
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
