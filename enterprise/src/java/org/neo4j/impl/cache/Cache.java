package org.neo4j.impl.cache;

/**
 * Simple cache interface with add, remove, get, clear and size methods.
 * If null is passed as parameter an {@link IllegalArgumentException} is thown.
 * <p>
 * If the cache cleans it self (for example a LIFO cache with maximum size) the 
 * <CODE>elementCleaned</CODE> method is invoked. Overide the default 
 * implementation (that does nothing) if needed.  
 * <p>
 * TODO: Create a pluggable, scalable, configurable, self analyzing/adaptive, 
 * statistics/reportable cache architecture. Emil will code that in four hours 
 * when he has time.
 */
public abstract class Cache<K,E>
{
	/**
	 * Returns the name of the cache. 
	 *
	 * @return name of the cache
	 */
	public abstract String getName();
	
	/**
	 * Adds <CODE>element</CODE> to cache.
	 *
	 * @param key the key for the element
	 * @param element the element to cache
	 */
	public abstract void add( K key, E element );
	
	/**
	 * Removes the element for <CODE>key</CODE> from cache and returns it. 
	 * If the no element for <CODE>key</CODE> exists <CODE>null</CODE> is 
	 * returned.
	 *
	 * @param key the key for the element
	 * @return the removed element or <CODE>null</CODE> if element didn't exist
	 */
	public abstract E remove( K key );
	
	/**
	 * Returns the cached element for <CODE>key</CODE>. If the element isn't 
	 * in cache <CODE>null</CODE> is returned. 
	 *
	 * @param key the key for the element
	 * @return the cached element or <CODE>null</CODE> if element didn't exist
	 */
	public abstract E get( K key );
	
	/**
	 * Removing all cached elements.
	 */
	public abstract void clear();
	
	/**
	 * Returns the cache size.
	 *
	 * @return cache size
	 */
	public abstract int size();
	
	/**
	 * If cache is self cleaning this method will be invoked with the element 
	 * cleaned. Overide this implementation (that does nothing) if needed.
	 *
	 * @param element cache element that has been removed
	 */
	protected void elementCleaned( E element )
	{
	}
	
	public abstract int maxSize();
	
	public abstract void resize( int newSize );
	
	private boolean isAdaptive = false;
	
	boolean isAdaptive()
	{
		return isAdaptive;
	}
	
	void setAdaptiveStatus( boolean status )
	{
		isAdaptive = status;
	}
}
