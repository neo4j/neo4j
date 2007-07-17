package org.neo4j.impl.nioneo.store;

import java.io.IOException;

/**
 * Common interface for the node,relationship,property and relationship
 * type stores.
 */
public interface Store
{
	/**
	 * Returns the id of next free record.
	 * 
	 * @return The id of the next free record
	 * @throws IOException If unable to 
	 */
	public int nextId() throws IOException;
	
	/**
	 * Throws a <CODE>RuntimeException</CODE> if store not ok or
	 * an IOException if something wrong with current transaction. 
	 */
	public void validate() throws IOException;

	public int getHighestPossibleIdInUse();
	
	public int getNumberOfIdsInUse();
}
