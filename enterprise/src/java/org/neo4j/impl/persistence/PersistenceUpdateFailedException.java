package org.neo4j.impl.persistence;

/**
 * Thrown if a persistence state update failed.
 */
public class PersistenceUpdateFailedException extends java.lang.Exception
{
	public PersistenceUpdateFailedException( String s )
	{
		super( s );
	}

	public PersistenceUpdateFailedException( String s, Throwable cause )
	{
		super( s, cause );
	}
}
