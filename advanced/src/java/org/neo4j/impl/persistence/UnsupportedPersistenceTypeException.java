package org.neo4j.impl.persistence;

/**
 * Thrown by a component in the persistence layer to indicate that
 * it has encountered an entity type that it does not know how to persist.
 */
class UnsupportedPersistenceTypeException extends java.lang.RuntimeException
{
	UnsupportedPersistenceTypeException( String s )
	{
		super( s );
	}

	UnsupportedPersistenceTypeException( String s, Throwable cause )
	{
		super( s, cause );
	}
}
