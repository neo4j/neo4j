package org.neo4j.impl.persistence;

/**
 * A data resource that the kernel uses to persist entities.
 * Examples of a <CODE>PersistenceSource</CODE> include a database server
 * or an LDAP server.
 * <P>
 * All persistence sources in the kernel should implement this interface.
 * Typically, a new persistence source is represented by one front-end class
 * that implements both <CODE>PersistenceSource</CODE> and
 * {@link com.windh.kernel.module.framework.KernelModule}. On bootstrap,
 * the persistence source generates a
 * {@link org.neo4j.impl.event.Event#DATA_SOURCE_ADDED DATA_SOURCE_ADDED}
 * event, which the persistence framework receives and uses to include the
 * persistence source in all future persistence operations.
 * <P>
 * For more information about implementing a kernel-compatible persistence
 * source, see the Persistence Layer chapter in the Kernel Documentation.
 */
public interface PersistenceSource
{
	/**
	 * Creates a resource connection to this persistence source.
	 * @return a newly opened {@link ResourceConnection} to this
	 * PersistenceSource
	 * @throws ConnectionCreationFailedException if unable to open a new
	 * ResourceConnection to this PersistenceSource
	 */
	public ResourceConnection createResourceConnection()
		throws ConnectionCreationFailedException;
		
	/**
	 * If the persistence source is responsible for id generation it must 
	 * implement this method. If the persistence source is unable to generate 
	 * id for any reason a {@link IdGenerationFailedException} should be thrown.
	 *
	 * @param clazz the data structure to get next free unique id for
	 * @return the next free unique id for <CODE>clazz</CODE>
	 */
	public int nextId( Class clazz );
}
