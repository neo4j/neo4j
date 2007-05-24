package org.neo4j.impl.persistence;

// Kernel imports
import org.neo4j.impl.event.EventListenerNotRegisteredException;

/**
 *
 * This class represents the persistence module. It receives lifecycle
 * events from the module framework.
 * @see com.windh.kernel.module.framework.KernelModule
 */
public class PersistenceModule
{
	// -- Constants
	private static final String	MODULE_NAME			= "PersistenceModule";
	
	// javadoc: see KernelModule
	public synchronized void init()
	{
		// Do nothing
	}
	
	// javadoc: see KernelModule
	public synchronized void start()
	{
		try
		{
			BusinessLayerMonitor.getMonitor().registerEvents();
		}
		catch ( EventListenerNotRegisteredException elnre )
		{
			// TODO:	This is a fatal thing. We really should shut down the
			//			kernel if something messes up during the startup of
			//			the persistence module. How do we tell the module
			//			framework to request emergency shutdown of the kernel?
			throw new RuntimeException( "The business layer monitor was " +
										  "unable to register on one or more " +
										  "event types. This can seriously " +
										  "impact persistence operations.",
										  elnre );
		}
	}
	
	// javadoc: see KernelModule
	public synchronized void reload()
	{
		this.stop();
		this.start();
	}
	
	// javadoc: see KernelModule
	public synchronized void stop()
	{
		BusinessLayerMonitor.getMonitor().unregisterEvents();
	}
	
	// javadoc: see KernelModule
	public synchronized void destroy()
	{
		// Do nothing
	}
	
	// javadoc: see KernelModule
	public String getModuleName()
	{
		return MODULE_NAME;
	}
}