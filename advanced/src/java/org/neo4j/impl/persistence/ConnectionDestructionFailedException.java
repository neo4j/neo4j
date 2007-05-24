package org.neo4j.impl.persistence;

/**
 * Thrown if a {@link ResourceConnection} is unable to destroy (close)
 * the underlying connection. This name officially stinks.
 */
public class ConnectionDestructionFailedException extends java.lang.Exception
{
	public ConnectionDestructionFailedException( String s )
	{
		super( s );
	}

	public ConnectionDestructionFailedException( String s, Throwable cause )
	{
		super( s, cause );
	}
}
