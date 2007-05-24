package org.neo4j.impl.event;


public class EventElement
{
	private Event event = null;
	private EventData data = null;
	private boolean proActive;

	EventElement( Event event, EventData data, boolean proActive )
	{
		this.event = event;
		this.data = data;
		this.proActive = proActive;
	}
	
	public Event getEvent()
	{
		return event;
	}
	
	public EventData getEventData()
	{
		return data;
	}
	
	public boolean isProActive()
	{
		return proActive;
	}
}
