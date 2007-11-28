package org.neo4j.util.matching;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Just a temporary utility for dealing with neo properties which are arrays.
 * Since the neo arrays are returned as fundamental types, f.ex. int[],
 * float[], String[] etc... And we'd like to deal with those as objects instead
 * so that an equals method may be used.
 */
public class NeoArrayPropertyUtil
{
	/**
	 * @param valueFromNeo neo node.getProperty value.
	 * @return a collection of all the values from a neo property. If the neo
	 * value is just a plain "single" value the collection will contain
	 * that single value. If the neo value is an array of values, all those
	 * values are added to the collection.
	 */
	public static Collection<Object> neoValueToCollection(
		Object valueFromNeo )
	{
		Set<Object> values = new HashSet<Object>();
		if ( valueFromNeo instanceof boolean[] )
		{
			values.addAll( Arrays.asList( ( boolean[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Boolean[] )
		{
			values.addAll( Arrays.asList( ( Boolean[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof byte[] )
		{
			values.addAll( Arrays.asList( ( byte[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Byte[] )
		{
			values.addAll( Arrays.asList( ( Byte[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof short[] )
		{
			values.addAll( Arrays.asList( ( short[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Short[] )
		{
			values.addAll( Arrays.asList( ( Short[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof int[] )
		{
			values.addAll( Arrays.asList( ( int[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Integer[] )
		{
			values.addAll( Arrays.asList( ( Integer[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof long[] )
		{
			values.addAll( Arrays.asList( ( long[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Long[] )
		{
			values.addAll( Arrays.asList( ( Long[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof float[] )
		{
			values.addAll( Arrays.asList( ( float[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Float[] )
		{
			values.addAll( Arrays.asList( ( Float[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof double[] )
		{
			values.addAll( Arrays.asList( ( double[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof Double[] )
		{
			values.addAll( Arrays.asList( ( Double[] ) valueFromNeo ) );
		}
		else if ( valueFromNeo instanceof String[] )
		{
			values.addAll( Arrays.asList( ( String[] ) valueFromNeo ) );
		}
		else
		{
			values.add( valueFromNeo );
		}
		return values;
	}
}
