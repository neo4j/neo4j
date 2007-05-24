package org.neo4j.impl.persistence;

public class PersistenceException extends java.lang.Exception
{
	public PersistenceException( String s )
	{
		super( s );
	}

	public PersistenceException( String s, Throwable cause )
	{
		super( s, cause );
	}
	
	public PersistenceException( Throwable cause )
	{
		super( cause );
	}
}
