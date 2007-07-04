package org.neo4j.impl.persistence;

/**
 * The IdGenerator is responsible for generating unique ids for entities in
 * the Neo. The IdGenerator is configured via the {@link IdGeneratorModule}.
 * <P>
 * The IdGenerator must be loaded after its designated
 * {@link IdGeneratorModule#setPersistenceSource persistence source} during
 * startup.
 * <P>
 */
public class IdGenerator
{
	// -- Singleton stuff
	private static IdGenerator instance = new IdGenerator();
	private IdGenerator() {}
	
	/**
	 * Singleton accessor.
	 * @return the singleton id generator
	 */
	public static IdGenerator getGenerator() { return instance; }
	
	
	// -- Attributes
	
	// the persistence source used to store the HIGH keys
	private PersistenceSource persistenceSource		= null;
	
	/**
	 * Returns the next unique ID for the entity type represented by
	 * <CODE>clazz</CODE>.
	 * @return the next ID for <CODE>clazz</CODE>'s entity type
	 * @throws IdGenerationFailedException if <CODE>clazz</CODE> is not
	 * a registered entity type or if communication with persistent
	 * storage failed
	 */
	public int nextId( Class clazz )
	{
		return getPersistenceSource().nextId( clazz );
	}
	
	
	// -- Configuration and attribute accessors
	
	/**
	 * Configures the IdGenerator. <B>WARNING</B>: This method should
	 * only be invoked once from {@link IdGeneratorModule#start}.
	 * @param source the persistence source used for id generation
	 */
	void configure( PersistenceSource source )
	{
		// Save connectivity
		this.persistenceSource = source;
	}
	
	// Accesor for persistence source
	private PersistenceSource getPersistenceSource()
	{
		return this.persistenceSource;
	}
}
