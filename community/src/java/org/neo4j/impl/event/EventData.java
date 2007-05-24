package org.neo4j.impl.event;

/**
 * <CODE>EventData</CODE> encapsulates data that is passed to the 
 * pro-active and re-active event listeners receiving the event. 
 */
public class EventData
{
	private Object data = null;
	private Thread originatingThread = null;
	
	/**
	 * Sets the data to encapsulate.
	 *
	 * @param data the event data
	 */
	public EventData( Object data )
	{
		this.data = data;
	}
	
	/**
	 * Sets the thread that originated this event.
	 * @param originatingThread the thread that originated this event
	 */
	void setOriginatingThread( Thread originatingThread )
	{
		this.originatingThread = originatingThread;
	}
	
	/**
	 * Returns the encapsulated data.
	 *
	 * @return the event data
	 */
	public Object getData()
	{
		return data;
	}

	/**
	 * Gets the thread that originated this event.
	 * @return the thread that originated this event
	 */
	public Thread getOriginatingThread()
	{
		return this.originatingThread;
	}
}