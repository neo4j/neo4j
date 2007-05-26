package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RemoteOutput extends UnicastRemoteObject implements Output
{
	private RemoteOutput() throws RemoteException
	{
		super();
	}
	
	public static RemoteOutput newOutput()
	{
		try
		{
			return new RemoteOutput();
		}
		catch ( RemoteException e )
		{
			throw new RuntimeException( e );
		}
	}
	
	public void print( Serializable object )
	{
		System.out.print( object );
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
	
	public static String asString( CharSequence sequence )
	{
		return sequence == null ? "null" : sequence.toString();
	}

	public Appendable append( CharSequence sequence )
	{
		this.println( asString( sequence ) );
		return this;
	}

	public Appendable append( CharSequence sequence, int start, int end )
	{
		this.print( asString( sequence ).substring( start, end ) );
		return this;
	}
}
