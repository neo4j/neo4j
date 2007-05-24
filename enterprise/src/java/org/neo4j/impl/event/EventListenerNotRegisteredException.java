package org.neo4j.impl.event;

public class EventListenerNotRegisteredException extends Exception
{
	public EventListenerNotRegisteredException()
	{
		super();
	}
	
	public EventListenerNotRegisteredException( String message )
	{
		super( message );
	}
	
	public EventListenerNotRegisteredException( String message, 
		Throwable cause)
	{
		super( message, cause );
	}

	public EventListenerNotRegisteredException( Throwable cause )
	{
		super( cause );
	}
}	