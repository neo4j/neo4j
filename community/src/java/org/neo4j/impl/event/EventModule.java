package org.neo4j.impl.event;

/**
 * This class represents the {@link EventManager} module. It receives lifecycle
 * events from the module framework and supports configuration of the
 * event module.
 */
public class EventModule
{
	private static final String	MODULE_NAME			= "EventModule";
	
	public void init()
	{
	}
	
	public void start()
	{
		EventManager.getManager().start();
	}
	
	public void reload()
	{
		EventManager.getManager().stop();
		EventManager.getManager().start();
	}
	
	public void stop()
	{
		EventManager.getManager().stop();
	}
	
	public void destroy()
	{
		EventManager.getManager().destroy();
	}
	
	public String getModuleName()
	{
		return MODULE_NAME;
	}
	
	public void setReActiveEventQueueWaitTime( int time )
	{
		EventManager.getManager().setReActiveEventQueueWaitTime( time );
	}
	
	public int getReActiveEventQueueWaitTime()
	{
		return EventManager.getManager().getReActiveEventQueueWaitTime();
	}
	
	public void setReActiveEventQueueNotifyOnCount( int count )
	{
		EventManager.getManager().setReActiveEventQueueNotifyOnCount( count );
	}
	
	public int getReActiveEventQueueNotifyOnCount()
	{
		return EventManager.getManager().getReActiveEventQueueNotifyOnCount();
	}
}