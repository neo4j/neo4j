package org.neo4j.impl.nioneo.store;

class MappedMemException extends RuntimeException
{
	MappedMemException( Throwable cause )
	{
		super( cause );
	}
}
