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
 * @see org.neo4j.impl.persistence.IdGenerator
 */
public class IdGeneratorModule
{
	// -- Constants
	private static final String	MODULE_NAME			= "IdGeneratorModule";
	
	// -- Attributes
	private PersistenceSource	persistenceSource	= null;
	
	
	public synchronized void init()
	{
		// Do nothing
	}
	
	public synchronized void start()
	{
		// Configure the IdGenerator
		IdGenerator.getGenerator().configure( this.getPersistenceSource() );
	}
	
	public synchronized void reload()
	{
		throw new RuntimeException( "IdGenerator does not support reload" );
	}
	
	public synchronized void stop()
	{
		// Do nothing
	}
	
	public synchronized void destroy()
	{
		// Do nothing
	}
	
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
		
		throw new RuntimeException( "Implement this" );
	}
	
	public synchronized void setPersistenceSourceInstance( 
		PersistenceSource source )
	{
		this.persistenceSource = source;
	}

	public String toString()
	{
		return this.getModuleName();
	}
}
