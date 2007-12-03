package org.neo4j.util.shell;

import java.io.Serializable;

/**
 * An implementation of {@link Output} optimized to use with a
 * {@link SameJvmClient}.
 */
public class SystemOutput implements Output
{
	public void print( Serializable object )
	{
		System.out.print( object );
	}
	
	public void println()
	{
		System.out.println();
	}

	public void println( Serializable object )
	{
		System.out.println( object );
	}

	public Appendable append( char ch )
	{
		this.print( ch );
		return this;
	}

	public Appendable append( CharSequence sequence )
	{
		this.println( RemoteOutput.asString( sequence ) );
		return this;
	}

	public Appendable append( CharSequence sequence, int start, int end )
	{
		this.print( RemoteOutput.asString( sequence ).substring( start, end ) );
		return this;
	}
}
