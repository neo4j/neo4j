package org.neo4j.graphmatching;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Just a temporary utility for dealing with Neo4j properties which are arrays.
 * Since the neo arrays are returned as fundamental types, f.ex. int[],
 * float[], String[] etc... And we'd like to deal with those as objects instead
 * so that an equals method may be used.
 */
public class ArrayPropertyUtil
{
	/**
	 * @param propertyValue neo node.getProperty value.
	 * @return a collection of all the values from a neo property. If the neo
	 * value is just a plain "single" value the collection will contain
	 * that single value. If the neo value is an array of values, all those
	 * values are added to the collection.
	 */
	public static Collection<Object> propertyValueToCollection(
		Object propertyValue )
	{
		Set<Object> values = new HashSet<Object>();
		try
		{
			int length = Array.getLength( propertyValue );
			for ( int i = 0; i < length; i++ )
			{
				values.add( Array.get( propertyValue, i ) );
			}
		}
		catch ( IllegalArgumentException e )
		{
			values.add( propertyValue );
		}
		return values;
	}
}
