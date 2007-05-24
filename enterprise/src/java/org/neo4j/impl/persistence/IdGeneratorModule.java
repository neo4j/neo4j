package org.neo4j.impl.persistence;

/**
 * This class represents the {@link IdGenerator} module. It receives lifecycle
 * events from the module framework and supports configuration of the
 * IdGenerator.
 * <P>
 * Generally, configuration of the IdGenerator must be done before the
 * first invocation of the {@link #start} method. This is because the
 * IdGenerator does not currently support the modular start/stop/reload
 * operations and there is no way to guarantee consistent behavior if
 * the IdGenerator is not halted during reconfiguration.
 * <P>
 * @see com.windh.kernel.module.framework.KernelModule
 * @see com.windh.kernel.persistence.IdGenerator
 */
public class IdGeneratorModule
{
	// -- Constants
	private static final String	MODULE_NAME			= "IdGeneratorModule";
	
	// -- Attributes
	private PersistenceSource	persistenceSource	= null;
	
	
	// -- Kernel module operations
	
	// javadoc: see KernelModule
	public synchronized void init()
	{
		// Do nothing
	}
	
	// javadoc: see KernelModule
	public synchronized void start()
	{
		// Configure the IdGenerator
		IdGenerator.getGenerator().configure( this.getPersistenceSource() );
	}
	
	// javadoc: see KernelModule
	public synchronized void reload()
	{
		throw new RuntimeException( "IdGenerator does not support reload" );
	}
	
	// javadoc: see KernelModule
	public synchronized void stop()
	{
		// Do nothing
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
	
	// -- Configuration operations
	
	// javadoc: see IdGeneratorModuleMBean
	private synchronized PersistenceSource getPersistenceSource()
	{
		return this.persistenceSource;
	}
	
	// javadoc: see IdGeneratorModuleMBean
	public synchronized void setPersistenceSource( String sourceName )
	{
		// Sanity check
		if ( this.getPersistenceSource() != null )
		{
			throw new UnsupportedOperationException( "PersistenceSource can " +
													 "not be changed during " +
													 "operations" );
		}
		
//		try
//		{
			// ModuleManager mgr = ModuleManager.getManager();
			// KernelModule source = mgr.getModuleByName( sourceName );
			throw new RuntimeException( "Implement this" );
//			if ( source instanceof PersistenceSource )
//			{
//				this.persistenceSource = (PersistenceSource) source;
//			}
//			else
//			{
//				// Throwing RuntimeExc is ok in an JMX-invoked method
//				throw new RuntimeException( sourceName + " is not a " +
//											"persistence source" );
//			}
//		}
	}
	
	public void setPersistenceSourceInstance( PersistenceSource source )
	{
		this.persistenceSource = source;
	}

	// -- Misc operations
	
	public String toString()
	{
		return this.getModuleName();
	}
}
