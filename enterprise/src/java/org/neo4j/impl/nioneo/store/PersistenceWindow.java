package org.neo4j.impl.nioneo.store;

/**
 * A persistence window encapsulates a part of the records (or blocks) in
 * a store and makes it possible to read and write data to those records. 
 */
public interface PersistenceWindow
{
	/**
	 * Returns the underlying buffer to this persistence window.
	 * 
	 * @return The underlying buffer
	 */
	public Buffer getBuffer();

	/**
	 * Returns the current record/block position.
	 * 
	 * @return The current position
	 */
	public int position();
	
	/**
	 * Returns the size of this window meaning the number of records/blocks it 
	 * encapsulates. 
	 * 
	 * @return The window size
	 */
	public int size();
}

