package org.neo4j.impl.persistence;

import org.neo4j.impl.event.EventListenerNotRegisteredException;

/**
 *
 * This class represents the persistence module. It receives lifecycle
 * events from the module framework.

 */
public class PersistenceModule
{
	// -- Constants
	private static final String	MODULE_NAME			= "PersistenceModule";
	
	public synchronized void init()
	{
		// Do nothing
	}
	
	public synchronized void start()
	{
		try
		{
			BusinessLayerMonitor.getMonitor().registerEvents();
		}
		catch ( EventListenerNotRegisteredException elnre )
		{
			// TODO:	This is a fatal thing. We really should shut down the
			//			Neo engine if something messes up during the startup of
			//			the persistence module. How do we tell the module
			//			framework to request emergency shutdown?
			throw new RuntimeException( "The business layer monitor was " +
										  "unable to register on one or more " +
										  "event types. This can seriously " +
										  "impact persistence operations.",
										  elnre );
		}
	}
	
	public synchronized void reload()
	{
		this.stop();
		this.start();
	}
	
	public synchronized void stop()
	{
		BusinessLayerMonitor.getMonitor().unregisterEvents();
	}
	
	public synchronized void destroy()
	{
		// Do nothing
	}
	
	public String getModuleName()
	{
		return MODULE_NAME;
	}
}