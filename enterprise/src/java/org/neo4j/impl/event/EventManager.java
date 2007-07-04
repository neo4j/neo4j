package org.neo4j.impl.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.logging.Logger;


/**
 * The EventManager is used to register/unregister event listeners
 * and to generate events. There are two types of events and event listeners, 
 * pro-active and re-active. A generated event consists of 
 * {@link Event event type} and {@link EventData event data} and will be sent 
 * to the event listeners registered on the event type.
 * <p>
 * Pro-active events are synchronous, generating a pro-active event
 * will tell all the pro-active event listeners registered on that event to 
 * negotiate and return an answer. This is a very useful way to ask
 * about something but the caller don't have to know who needs to be asked. 
 * Generate a pro-active event and continue depending on what answer you get.
 * <p>
 * The other type of events, re-active events, are asynchronous. Generating a 
 * re-active event will just place the event in a queue. The re-active events
 * are removed from the queue in the near future and sent to the re-active 
 * event listeners registered on that event. This is a good way to tell 
 * someone that something has been done without knowing who you have to tell.
 */
public class EventManager
{
	private static Logger log =
		Logger.getLogger( EventManager.class.getName() );
	
	private static EventManager instance = new EventManager();
	
	private Map<Event,List<ProActiveEventListener>> proActiveEventListeners = 
		new HashMap<Event,List<ProActiveEventListener>>();
	private Map<Event,List<ReActiveEventListener>> reActiveEventListeners = 
		new HashMap<Event,List<ReActiveEventListener>>();
	private LinkedList<EventElement> eventElements = 
		new LinkedList<EventElement>();
	private boolean startIsOk = true;
	private boolean destroyed = true;	

	private EventQueue eventQueue		= new EventQueue();
	
	private EventManager()
	{
	}
	
	/**
	 * Returns the single instance of this class.
	 *
	 * @return the <CODE>EventManager</CODE> instance
	 */
	public static EventManager getManager()
	{
		return instance;
	}
	
	/**
	 * Generates a pro-active event to all pro-active event listeners 
	 * registered on <CODE>event</CODE>. The event listeners will negotiate
	 * and an answer will be returned, synchronously.
	 * <p>
	 * The results returned from the registered event listeners are anded
	 * and returned, if there are no pro-active event listeners registered
	 * on <CODE>event</CODE>, <CODE>true</CODE> will be returned.
	 * 
	 * @param event the event type
	 * @param data the event data
	 * @return the negotiated answers from the event listeners
	 */
	public boolean generateProActiveEvent( Event event,	EventData data )
	{
		checkIfDestroyed();
		
		List<ProActiveEventListener> listenerList = 
			proActiveEventListeners.get( event );
		if ( listenerList != null )
		{
			Iterator<ProActiveEventListener> listItr = listenerList.iterator();
			boolean result = true;
			// no concurrent mod, list is copied in modyfing blocks
			while ( listItr.hasNext() && result )
			{
				ProActiveEventListener listener = listItr.next();
				try
				{
					if ( !listener.proActiveEventReceived( event, data ) )
					{
						result = false;
					}
				}
				catch ( Throwable t )
				{
					t.printStackTrace();
					log.severe( "Exception sending pro-active event to " + 
						listener );
					result = false;
				}
			}
			return result;
		}
		return true;
	}
	
	/**
	 * Generates a re-active event to all re-active event listeners 
	 * registered on <CODE>event</CODE>. The event will be sent to the 
	 * listeners in the near future, asynchronous communication.
	 * 
	 * @param event the event type
	 * @param data the event data
	 */
	public void generateReActiveEvent( Event event, EventData data )
	{
		checkIfDestroyed();
		markWithOriginatingThread( data );
		EventElement evtElement = new EventElement( event, data, false );
		synchronized ( eventElements )
		{
			eventElements.add( evtElement );
		}
	}
	
	EventElement getNextEventElement()
	{
		if ( eventElements == null )
		{
			return null;
		}
		synchronized ( eventElements )
		{
			if ( !startIsOk && !eventElements.isEmpty() )
			{
				try
				{
					return eventElements.removeFirst();
				}
				catch ( NoSuchElementException e )
				{
					// ok return null
				}
			}
		}
		return null;
	}

	/**
	 * Registers a pro-active event listener to <CODE>event</CODE>. 
	 *
	 * @param listener the pro-active event listener to register
	 * @param event the event to register <CODE>listener</CODE> to
	 * @throws EventListenerAlreadyRegisteredException if the 
	 * <CODE>listener</CODE> is already registered
	 * @throws EventListenerNotRegisteredException if <CODE>listener</CODE> or
	 * <CODE>event</CODE> is <CODE>null</CODE>
	 */
	public synchronized void registerProActiveEventListener( 
		ProActiveEventListener listener, Event event  )
		throws EventListenerAlreadyRegisteredException, 
		EventListenerNotRegisteredException
	{
		checkIfDestroyed();
		if ( listener == null || event == null )
		{
			throw new EventListenerNotRegisteredException( 
				"Null parameter, listener=" + listener + ", event=" +
				event );
		}
		if ( proActiveEventListeners.containsKey( event ) )
		{
			List<ProActiveEventListener> listenerList = 
				proActiveEventListeners.get( event );
			if ( !listenerList.contains( listener ) )
			{
				List<ProActiveEventListener> newList = 
					new ArrayList<ProActiveEventListener>();
				newList.addAll( listenerList );
				newList.add( listener );
				proActiveEventListeners.put( event, newList );
			}
			else 
			{
				throw new EventListenerAlreadyRegisteredException( 
					" listener=" + listener + ", event=" + event );
			}
		}
		else
		{
			List<ProActiveEventListener> listenerList = 
				new ArrayList<ProActiveEventListener>();
			listenerList.add( listener );
			proActiveEventListeners.put( event, listenerList );
		}
	}
	
	/**
	 * Removes a pro-active event listener from <CODE>event</CODE>.
	 *
	 * @param listener the pro-active event listener
	 * @param event the event to remove <CODE>listener</CODE> from
	 * @throws EventListenerNotRegisteredException if <CODE>listener</CODE>
	 * is no registered
	 */
	public synchronized void unregisterProActiveEventListener(
		ProActiveEventListener listener, Event event )
		throws EventListenerNotRegisteredException
	{
		checkIfDestroyed();
		if ( proActiveEventListeners.containsKey( event ) )
		{
			List<ProActiveEventListener> listenerList = 
				proActiveEventListeners.get( event );
			if ( listenerList.contains( listener ) )
			{
				List<ProActiveEventListener> newList = 
					new ArrayList<ProActiveEventListener>();
				newList.addAll( listenerList );
				newList.remove( listener );
				proActiveEventListeners.put( event, newList );
				if ( newList.size() == 0 )
				{
					proActiveEventListeners.remove( event );
				}
			}
			else 
			{
				throw new EventListenerNotRegisteredException(
					" listener=" + listener + ", event=" + event );
			}
		}
		else
		{
			throw new EventListenerNotRegisteredException(
				" listener=" + listener + ", event=" + event );
		}
	}
	
	/**
	 * Registers a re-active event listener to <CODE>event</CODE>. 
	 *
	 * @param listener the re-active event listener to register
	 * @param event the event to register <CODE>listener</CODE> to
	 * @throws EventListenerAlreadyRegisteredException if the 
	 * <CODE>listeners</CODE> is already registered
	 * @throws EventListenerNotRegisteredException if <CODE>listener</CODE> or
	 * <CODE>event</CODE> is <CODE>null</CODE>
	 */
	public synchronized void registerReActiveEventListener( 
		ReActiveEventListener listener, Event event )
		throws EventListenerAlreadyRegisteredException, 
		EventListenerNotRegisteredException
	{
		checkIfDestroyed();
		if ( listener == null || event == null )
		{
			throw new EventListenerNotRegisteredException( 
				"Null parameter, listener=" + listener + ", event=" +
				event );
		}
		if ( reActiveEventListeners.containsKey( event ) )
		{
			List<ReActiveEventListener> listenerList = 
				reActiveEventListeners.get( event );
			if ( !listenerList.contains( listener ) )
			{
				
				List<ReActiveEventListener> newList = 
					new ArrayList<ReActiveEventListener>();
				newList.addAll( listenerList );
				newList.add( listener );
				reActiveEventListeners.put( event, newList );
			}
			else 
			{
				throw new EventListenerAlreadyRegisteredException( 
					" listener=" + listener + ", event=" + event );
			}
		}
		else
		{
			List<ReActiveEventListener> listenerList = 
				new ArrayList<ReActiveEventListener>();
			listenerList.add( listener );
			reActiveEventListeners.put( event, listenerList );
		}
	}
	
	/**
	 * Removes a re-active event listener from <CODE>event</CODE>.
	 *
	 * @param listener the re-active event listener
	 * @param event the event to remove <CODE>listener</CODE> from
	 * @throws EventListenerNotRegisteredException if <CODE>listener</CODE>
	 * is no registered
	 */
	public synchronized void unregisterReActiveEventListener( 
		ReActiveEventListener listener, Event event )
		throws EventListenerNotRegisteredException
	{
		checkIfDestroyed();
		if ( reActiveEventListeners.containsKey( event ) )
		{
			List<ReActiveEventListener> listenerList = 
				reActiveEventListeners.get( event );
			if ( listenerList.contains( listener ) )
			{
				List<ReActiveEventListener> newList = 
					new ArrayList<ReActiveEventListener>();
				newList.addAll( listenerList );
				newList.remove( listener );
				reActiveEventListeners.put( event, newList );
				if ( newList.size() == 0 )
				{
					reActiveEventListeners.remove( event );
				}
			}
			else 
			{
				throw new EventListenerNotRegisteredException(
					" listener=" + listener + ", event=" + event );
			}
		}
		else
		{
			throw new EventListenerNotRegisteredException(
				" listener=" + listener + ", event=" + event );
		}
	}
	
	void sendReActiveEvent( Event event, EventData data )
	{
		List<ReActiveEventListener> listeners = 
			reActiveEventListeners.get( event );
		if ( listeners != null )
		{
			Iterator<ReActiveEventListener> listItr = listeners.iterator();
			while ( listItr.hasNext() )
			{
				ReActiveEventListener listener = listItr.next();
				try
				{
					listener.reActiveEventReceived( event, data );
				}
				catch ( Throwable t )
				{
					t.printStackTrace();
					log.severe( "Exception sending re-active event to " + 
						listener );
				}
			}
		}
	}
	
	synchronized void start()
	{
		if ( startIsOk )
		{
			eventQueue.start();
			startIsOk = false;
			destroyed = false;
		}
		else
		{
			log.warning( "EventModule already started" );
		}
	}
	
	synchronized void stop()
	{
		checkIfDestroyed();
		if ( !startIsOk )
		{
			startIsOk = true;
			eventQueue.shutdown();
			eventQueue = new EventQueue();
		}
		else
		{
			log.warning( "EventModule already stoped" );
		}
	}
	
	synchronized void destroy()
	{
		checkIfDestroyed();
		if ( startIsOk )
		{
			removeListeners();
			reActiveEventListeners = null;
			eventElements = null;
			destroyed = true;
			proActiveEventListeners = 
				new HashMap<Event,List<ProActiveEventListener>>();
			reActiveEventListeners = 
				new HashMap<Event,List<ReActiveEventListener>>();
			eventElements = 
				new LinkedList<EventElement>();
			startIsOk = true;
			destroyed = true;	
		}
		else
		{
			log.severe( "EventModule not in stopped state" );
		}
	}
	
	void setReActiveEventQueueWaitTime( int time )
	{
		checkIfDestroyed();
		eventQueue.setWaitTime( time );
	}
	
	int getReActiveEventQueueWaitTime()
	{
		checkIfDestroyed();
		return eventQueue.getWaitTime();
	}
	
	void setReActiveEventQueueNotifyOnCount( int count )
	{
		checkIfDestroyed();
		eventQueue.setNotifyOnCount( count );
	}
	
	int getReActiveEventQueueNotifyOnCount()
	{
		checkIfDestroyed();
		return eventQueue.getNotifyOnCount();
	}
	
	private void checkIfDestroyed()
	{
		if ( destroyed )
		{
			throw new RuntimeException( "EventModule has been destroyed." );
		}
	}
	
	private void markWithOriginatingThread( EventData data )
	{
		if ( data != null )
		{
			data.setOriginatingThread( Thread.currentThread() );
		}
	}

	private void removeListeners()
	{
		Iterator<Event> itr = 
			proActiveEventListeners.keySet().iterator();
		while ( itr.hasNext() )
		{
			Event event = itr.next();
			List<ProActiveEventListener> listenerList = 
				proActiveEventListeners.get( event );
			StringBuffer stringList = new StringBuffer();
			for ( int i = 0; i < listenerList.size(); i++ )
			{
				stringList.append( listenerList.get( i ) );
				stringList.append( "\n" );
			}
		}
		itr = reActiveEventListeners.keySet().iterator();
		while ( itr.hasNext() )
		{
			Event event = itr.next();
			List<ReActiveEventListener> listenerList = 
				reActiveEventListeners.get( event );
			StringBuffer stringList = new StringBuffer();
			for ( int i = 0; i < listenerList.size(); i++ )
			{
				stringList.append( listenerList.get( i ) );
				stringList.append( "\n" );
			}
		}
	}
}	