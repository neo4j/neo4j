package org.neo4j.impl.persistence;

/**
 *
 * An interface that provides meta data required by the persistence layer
 * about a persistent entity. The persistence layer uses this meta data to
 * for example select the data source that is used to persist the entity that
 * implements this interface.
 * <P>
 * Currently, this interface only contains an operation to obtain a reference
 * to the persistent entity.
 */
public interface PersistenceMetadata
{
	/**
	 * Returns the entity that wishes to participate in a persistence operation.
	 * @return the soon-to-be persistent entity
	 */
	public Object getEntity();
}
