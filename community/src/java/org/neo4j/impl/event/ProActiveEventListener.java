package org.neo4j.impl.event;


/** 
 * Pro-active event listeners should implement this interface. When a 
 * pro-active event is generated the <CODE>proActiveEventReceived</CODE> method
 * will be invoked on all pro-active listeners registered on that event. An 
 * answer is to be returned depending on what event and event data was received.
 */
public interface ProActiveEventListener
{
	// EE:	The contract should specify whether event and data can ever
	//		be null
	/**
	 * Invoked if <CODE>this</CODE> is registered on event type 
	 * <CODE>event</CODE> and an event of type <CODE>event</CODE> has been 
	 * generated.
	 * <p>
	 * The answer returned should be <CODE>true</CODE> if the generated
	 * event is of no interest for <CODE>this</CODE> event listener.
	 *
	 * @param event the generated event type
	 * @param data the event data
	 * @return <CODE>false</CODE> if not approved, <CODE>true</CODE> if 
	 * approved or no opinion.
	 */
	public boolean proActiveEventReceived( Event event, EventData data );
}