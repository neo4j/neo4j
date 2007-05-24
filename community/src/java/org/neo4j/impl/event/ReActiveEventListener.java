package org.neo4j.impl.event;


/** 
 * Re-active event listeners should implement this interface. When a 
 * re-active event is generated the <CODE>reActiveEventReceived</CODE> method
 * will be invoked on all re-active listeners registered on that event. 
 */
public interface ReActiveEventListener
{
	// EE:	The contract should specify whether event and data can ever
	//		be null
	/**
	 * Invoked if <CODE>this</CODE> is registered on event type 
	 * <CODE>event</CODE> and an event of type <CODE>event</CODE> has been 
	 * generated.
	 *
	 * @param event the generated event type
	 * @param data the event data
	 */
	public void reActiveEventReceived( Event event, EventData data );
}