package org.neo4j.impl.persistence;

/**
 * Thrown if the {@link ResourceBroker} is unable to acquire a
 * resource for a certain set of parameters.
 */
class ResourceAcquisitionFailedException extends java.lang.Exception
{
	ResourceAcquisitionFailedException( String s )
	{
		super( s );
	}

	ResourceAcquisitionFailedException( String s, Throwable cause )
	{
		super( s, cause );
	}
}
