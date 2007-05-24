package org.neo4j.impl.persistence;

/**
 * Thrown by the {@link IdGenerator} to indicate a failure in
 * ID generation for an entity.
 */
public class IdGenerationFailedException extends java.lang.RuntimeException
{
	public IdGenerationFailedException( Throwable cause )
	{
		super( cause );
	}

	public IdGenerationFailedException( String s )
	{
		super( s );
	}

	public IdGenerationFailedException( String s, Throwable cause )
	{
		super( s, cause );
	}
}
