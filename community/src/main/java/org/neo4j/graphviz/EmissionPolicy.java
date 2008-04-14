package org.neo4j.graphviz;

/**
 * An interface for querying what aspects of a graph to emit.
 * @author Tobias Ivarsson
 */
public interface EmissionPolicy
{
	/**
	 * A predefined default {@link EmissionPolicy} that accepts any query.
	 */
	static final EmissionPolicy ACCEPT_ALL = new EmissionPolicy()
	{
		public boolean acceptProperty( SourceType source, String key )
		{
			return true;
		}
	};

	/**
	 * Query if this property should be emitted or not.
	 * @param source
	 *            {@link SourceType#NODE} if the property is a node property,
	 *            {@link SourceType#RELATIONSHIP} if the property is a
	 *            relationship property.
	 * @param key
	 *            The property name.
	 * @return <code>true</code> if this property should be emitted,
	 *         <code>false</code> if not.
	 */
	boolean acceptProperty( SourceType source, String key );
}
