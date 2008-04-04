package org.neo4j.graphviz;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Stack;

import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Traverser;

public class GraphvizWriter extends EmittingConsumer
{
	private final GraphvizEmitter emitter;

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

	public GraphvizWriter( File file ) throws FileNotFoundException
	{
		this( new FileOutputStream( file ) );
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
		private final Stack<String> endTokens = new Stack<String>()
		{
			{
				push( "}" );
			}
		};
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
