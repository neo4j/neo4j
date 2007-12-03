package org.neo4j.util.shell;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * An implementation of {@link Output} which outputs over RMI to from the
 * server to the client.
 */
public class RemoteOutput extends UnicastRemoteObject implements Output
{
	private RemoteOutput() throws RemoteException
	{
		super();
	}
	
	/**
	 * Convenient method for creating a new instance without having to catch
	 * the {@link RemoteException}
	 * @return a new {@link RemoteOutput} instance.
	 */
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
	
	/**
	 * A util method for getting the correct string for {@code sequence},
	 * see {@link Appendable}.
	 * @param sequence the string value.
	 * @return the correct string.
	 */
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
