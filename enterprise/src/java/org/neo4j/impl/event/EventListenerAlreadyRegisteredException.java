package org.neo4j.impl.event;

public class EventListenerAlreadyRegisteredException extends Exception
{
	public EventListenerAlreadyRegisteredException()
	{
		super();
	}
	
	public EventListenerAlreadyRegisteredException( String message )
	{
		super( message );
	}
	
	public EventListenerAlreadyRegisteredException( String message, 
		Throwable cause)
	{
		super( message, cause );
	}

	public EventListenerAlreadyRegisteredException( Throwable cause )
	{
		super( cause );
	}
}	