package org.neo4j.impl.persistence;

/**
 * Thrown if a {@link PersistenceSource} is unable to create a
 * {@link ResourceConnection}.
 *
 * @see PersistenceSource
 * @see ResourceConnection
 */
public class ConnectionCreationFailedException extends java.lang.Exception
{
	public ConnectionCreationFailedException( String s )
	{
		super( s );
	}

	public ConnectionCreationFailedException( String s, Throwable cause )
	{
		super( s, cause );
	}
}
